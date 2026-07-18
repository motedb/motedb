//! Bug-hunt v12: COUNT(*) consistency, JOIN edge cases, cross-restart
//! aggregate correctness, and stress patterns.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db.execute(sql).unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize().unwrap_or_else(|e| panic!("mat failed: {}\n  err: {}", sql, e));
    match rs { QueryResult::Select { rows, .. } => rows, _ => panic!("not Select") }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("int? {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. COUNT(*) matches actual row count after mixed ops
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_matches_after_inserts_and_deletes() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Insert 100, delete 30, insert 20 more.
    for i in 1..=100 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
    for i in 1..=30 { exec(&db, &format!("DELETE FROM t WHERE id = {}", i)); }
    for i in 101..=120 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
    // Expected: 100 - 30 + 20 = 90.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 90);
    // Verify by scanning all IDs.
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r.len(), 90, "scan count must match COUNT(*)");
}

#[test]
fn count_after_update_unchanged() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=50 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
    // UPDATE doesn't change row count.
    exec(&db, "UPDATE t SET v = v + 1000");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 50);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. JOIN projection with aliases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_select_specific_columns() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)");
    exec(&db, "INSERT INTO a VALUES (1, 'Alice')");
    exec(&db, "INSERT INTO b VALUES (1, 1, 100)");
    // SELECT specific columns from both tables.
    let r = rows(&db, "SELECT name, val FROM a INNER JOIN b ON a.id = b.a_id");
    assert_eq!(r.len(), 1);
    match (&r[0][0], &r[0][1]) {
        (Value::Text(n), Value::Integer(v)) => { assert_eq!(&*n.0, "Alice"); assert_eq!(*v, 100); }
        o => panic!("{:?}", o),
    }
}

#[test]
fn join_three_tables() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, a_id INT)");
    exec(&db, "CREATE TABLE c (id INT PRIMARY KEY, b_id INT)");
    exec(&db, "INSERT INTO a VALUES (1)");
    exec(&db, "INSERT INTO b VALUES (1, 1)");
    exec(&db, "INSERT INTO c VALUES (1, 1)");
    // 3-table chain join.
    let r = rows(&db, "SELECT a.id FROM a INNER JOIN b ON a.id = b.a_id INNER JOIN c ON b.id = c.b_id");
    assert_eq!(r.len(), 1, "3-table chain join");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Aggregate consistency across reopen
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn aggregate_consistent_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let (count_before, sum_before, min_before, max_before);
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        for i in 1..=100 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 3)); }
        db.checkpoint().unwrap();
        count_before = scalar_i64(&db, "SELECT COUNT(*) FROM t");
        sum_before = scalar_i64(&db, "SELECT SUM(v) FROM t");
        min_before = scalar_i64(&db, "SELECT MIN(v) FROM t");
        max_before = scalar_i64(&db, "SELECT MAX(v) FROM t");
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), count_before);
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), sum_before);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), min_before);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), max_before);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. WHERE with text containing special chars
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_text_with_spaces() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello world')");
    exec(&db, "INSERT INTO t VALUES (2, 'hello')");
    let r = rows(&db, "SELECT id FROM t WHERE s = 'hello world'");
    assert_eq!(r.len(), 1);
}

#[test]
fn where_text_with_numbers_as_string() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, code TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '12345')");
    exec(&db, "INSERT INTO t VALUES (2, 'abc')");
    let r = rows(&db, "SELECT id FROM t WHERE code = '12345'");
    assert_eq!(r.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. GROUP BY with text column + multiple aggregates
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn group_by_text_multi_agg_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, salary INT)");
    exec(&db, "INSERT INTO t VALUES (1, 'eng', 100)");
    exec(&db, "INSERT INTO t VALUES (2, 'eng', 200)");
    exec(&db, "INSERT INTO t VALUES (3, 'eng', 300)");
    exec(&db, "INSERT INTO t VALUES (4, 'sales', 50)");
    exec(&db, "INSERT INTO t VALUES (5, 'sales', 150)");
    let r = rows(&db, "SELECT dept, COUNT(*), SUM(salary), MIN(salary), MAX(salary) FROM t GROUP BY dept ORDER BY dept");
    assert_eq!(r.len(), 2);
    // eng: count=3, sum=600, min=100, max=300.
    match (&r[0][0], &r[0][1], &r[0][2], &r[0][3], &r[0][4]) {
        (Value::Text(d), Value::Integer(c), Value::Integer(s), Value::Integer(mn), Value::Integer(mx))
            => {
            assert_eq!(&*d.0, "eng");
            assert_eq!(*c, 3);
            assert_eq!(*s, 600);
            assert_eq!(*mn, 100);
            assert_eq!(*mx, 300);
        }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Delete all then COUNT (counter reset correctness)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_all_count_zero_then_reinsert() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=50 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i)); }
    exec(&db, "DELETE FROM t");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 0);
    // Reinsert and verify count.
    exec(&db, "INSERT INTO t VALUES (1, 99)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 99);
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. SUM of all negative values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_all_negative() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 { exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, -i * 10)); }
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), -550);
    assert_eq!(scalar_i64(&db, "SELECT MIN(v) FROM t"), -100);
    assert_eq!(scalar_i64(&db, "SELECT MAX(v) FROM t"), -10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Multiple GROUP BY queries in sequence (cache isolation)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multiple_group_by_queries_no_interference() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=20 {
        let cat = match i % 3 { 0 => "x", 1 => "y", _ => "z" };
        exec(&db, &format!("INSERT INTO t VALUES ({}, '{}', {})", i, cat, i));
    }
    // Run multiple GROUP BY queries — results must be independent.
    let r1 = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY cat");
    let r2 = rows(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY cat");
    let r3 = rows(&db, "SELECT cat, MIN(v) FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(r1.len(), 3);
    assert_eq!(r2.len(), 3);
    assert_eq!(r3.len(), 3);
    // Verify SUM and COUNT are consistent: SUM of counts = 20.
    let total_count: i64 = r2.iter().filter_map(|r| match &r[1] {
        Value::Integer(n) => Some(*n), _ => None
    }).sum();
    assert_eq!(total_count, 20);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. WHERE with column IN (subquery) correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_in_subquery_correct() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, ref INT)");
    exec(&db, "INSERT INTO a VALUES (1, 100)");
    exec(&db, "INSERT INTO a VALUES (2, 200)");
    exec(&db, "INSERT INTO a VALUES (3, 300)");
    exec(&db, "INSERT INTO b VALUES (1, 2)");
    exec(&db, "INSERT INTO b VALUES (2, 3)");
    // a.id IN (SELECT ref FROM b) → ids 2, 3.
    let r = rows(&db, "SELECT v FROM a WHERE id IN (SELECT ref FROM b) ORDER BY v");
    assert_eq!(r.len(), 2);
    match (&r[0][0], &r[1][0]) {
        (Value::Integer(v1), Value::Integer(v2)) => { assert_eq!(*v1, 200); assert_eq!(*v2, 300); }
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Large INSERT batch then SUM verification
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn large_batch_sum_exact() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let n = 1000;
    for i in 1..=n {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // SUM(1..1000) = 500500.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 500500);
    // COUNT = 1000.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), n);
    // AVG ≈ 500.5.
    let r = rows(&db, "SELECT AVG(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((*f - 500.5).abs() < 0.01),
        Value::Integer(n) => panic!("AVG should be Float 500.5, got Integer {}", n),
        o => panic!("{:?}", o),
    }
}
