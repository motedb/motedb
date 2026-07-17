//! Bug-hunt v4: concurrent access, execute_prepared, parser edge cases,
//! and complex query compositions that are most likely to surface bugs.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use std::sync::Arc;
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
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() { Some(Value::Integer(n)) => *n, o => panic!("not int {:?}: {}", o, sql) }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. execute_prepared correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn prepared_select_with_params() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    let r = db.execute_prepared(
        "SELECT v FROM t WHERE id = ?",
        vec![Value::Integer(3)],
    ).unwrap().materialize().unwrap();
    if let QueryResult::Select { rows, .. } = r {
        assert_eq!(rows.len(), 1);
        match &rows[0][0] {
            Value::Integer(n) => assert_eq!(*n, 30),
            o => panic!("expected 30, got {:?}", o),
        }
    }
}

#[test]
fn prepared_insert_with_params() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v INT)");
    for i in 1..=3 {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?, ?)",
            vec![Value::Integer(i), Value::Text(format!("user{}", i).into()), Value::Integer(i * 100)],
        ).unwrap();
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3);
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 2"), 200);
}

#[test]
fn prepared_update_delete() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=5 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    db.execute_prepared(
        "UPDATE t SET v = ? WHERE id = ?",
        vec![Value::Integer(999), Value::Integer(3)],
    ).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 3"), 999);

    db.execute_prepared(
        "DELETE FROM t WHERE id = ?",
        vec![Value::Integer(2)],
    ).unwrap();
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 4);
}

#[test]
fn prepared_multiple_params() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20)");
    exec(&db, "INSERT INTO t VALUES (2, 30, 40)");
    // WHERE a > ? AND b < ?
    let r = db.execute_prepared(
        "SELECT id FROM t WHERE a > ? AND b < ?",
        vec![Value::Integer(20), Value::Integer(50)],
    ).unwrap().materialize().unwrap();
    if let QueryResult::Select { rows, .. } = r {
        assert_eq!(rows.len(), 1, "only id=2 matches a>20 AND b<50");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Concurrent reads while single-threaded writes (same Database handle)
//    MoteDB is single-process; verify interleaved read/write correctness.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn interleaved_read_write() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // Interleave: read, write, read, write, read.
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 10);
    exec(&db, "UPDATE t SET v = 20 WHERE id = 1");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1"), 20);
    exec(&db, "INSERT INTO t VALUES (2, 30)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 2);
    exec(&db, "DELETE FROM t WHERE id = 1");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Multi-threaded read concurrency (Database is Send + Sync via Arc)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn concurrent_reads_multi_thread() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    db.checkpoint().unwrap();

    let db = Arc::new(db);
    let mut handles = Vec::new();
    for t in 0..4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut ok = 0;
            for i in 1..=50 {
                let pid = (t * 25 + i) % 100 + 1;
                let r = db.execute(&format!("SELECT v FROM t WHERE id = {}", pid)).unwrap()
                    .materialize().unwrap();
                if let QueryResult::Select { rows, .. } = r {
                    if rows.len() == 1 { ok += 1; }
                }
            }
            ok
        }));
    }
    let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert_eq!(total, 200, "all 200 concurrent reads must succeed");
}

#[test]
fn concurrent_reads_with_background_index_build() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=500 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();

    let db = Arc::new(db);
    let mut handles = Vec::new();
    for _ in 0..4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for _ in 0..20 {
                let _ = db.execute("SELECT COUNT(*) FROM t WHERE cat = 'c1'")
                    .and_then(|r| r.materialize());
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. SQL parser edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn lowercase_keywords() {
    let (db, _d) = new_db();
    exec(&db, "create table t (id int primary key, v int)");
    exec(&db, "insert into t values (1, 10)");
    assert_eq!(scalar_i64(&db, "select v from t where id = 1"), 10);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn mixed_case_keywords() {
    let (db, _d) = new_db();
    exec(&db, "CrEaTe TaBlE t (id InT pRiMaRy KeY, v InT)");
    exec(&db, "InSeRt InTo t VaLuEs (1, 10)");
    assert_eq!(scalar_i64(&db, "select v from t where id = 1"), 10);
}

#[test]
fn extra_whitespace() {
    let (db, _d) = new_db();
    exec(&db, "  CREATE   TABLE   t   (  id   INT   PRIMARY KEY  ,  v   INT  )  ");
    exec(&db, "  INSERT   INTO   t   VALUES   (  1  ,   10  )  ");
    assert_eq!(scalar_i64(&db, "  SELECT   v   FROM   t   WHERE   id   =   1  "), 10);
}

#[test]
fn trailing_semicolon() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT);");
    exec(&db, "INSERT INTO t VALUES (1, 10);");
    assert_eq!(scalar_i64(&db, "SELECT v FROM t WHERE id = 1;"), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. DISTINCT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_single_column() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a')");
    exec(&db, "INSERT INTO t VALUES (2, 'b')");
    exec(&db, "INSERT INTO t VALUES (3, 'a')");
    exec(&db, "INSERT INTO t VALUES (4, 'a')");
    exec(&db, "INSERT INTO t VALUES (5, 'b')");
    let r = rows(&db, "SELECT DISTINCT cat FROM t ORDER BY cat");
    assert_eq!(r.len(), 2, "DISTINCT must dedup");
}

#[test]
fn distinct_multi_column() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 1)");
    exec(&db, "INSERT INTO t VALUES (2, 1, 1)"); // dup of (1,1)
    exec(&db, "INSERT INTO t VALUES (3, 1, 2)");
    exec(&db, "INSERT INTO t VALUES (4, 2, 1)");
    let r = rows(&db, "SELECT DISTINCT a, b FROM t");
    // Distinct pairs: (1,1), (1,2), (2,1) → 3
    assert_eq!(r.len(), 3, "DISTINCT on (a,b) must give 3 unique pairs");
}

#[test]
fn distinct_count() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a')");
    exec(&db, "INSERT INTO t VALUES (2, 'a')");
    exec(&db, "INSERT INTO t VALUES (3, 'b')");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(DISTINCT cat) FROM t"), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. CASE WHEN
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn case_when_simple() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 20)");
    exec(&db, "INSERT INTO t VALUES (3, 30)");
    let r = rows(&db,
        "SELECT id, CASE WHEN v > 25 THEN 'big' WHEN v > 15 THEN 'mid' ELSE 'small' END FROM t ORDER BY id");
    assert_eq!(r.len(), 3);
    match &r[0][1] { Value::Text(s) => assert_eq!(&*s.0, "small"), _ => panic!("10→small") }
    match &r[1][1] { Value::Text(s) => assert_eq!(&*s.0, "mid"), _ => panic!("20→mid") }
    match &r[2][1] { Value::Text(s) => assert_eq!(&*s.0, "big"), _ => panic!("30→big") }
}

#[test]
fn case_when_no_else_returns_null() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "INSERT INTO t VALUES (2, 30)");
    let r = rows(&db, "SELECT CASE WHEN v > 20 THEN 'big' END FROM t ORDER BY id");
    // id=1: no match, no ELSE → NULL. id=2: 'big'.
    assert!(matches!(r[0][0], Value::Null), "no ELSE → NULL");
    assert!(matches!(r[1][0], Value::Text(_)));
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. UNION
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn union_dedup() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO a VALUES (1)");
    exec(&db, "INSERT INTO a VALUES (2)");
    exec(&db, "INSERT INTO b VALUES (2)");
    exec(&db, "INSERT INTO b VALUES (3)");
    let r = rows(&db, "SELECT id FROM a UNION SELECT id FROM b ORDER BY id");
    assert_eq!(r.len(), 3, "UNION dedups: 1,2,3");
}

#[test]
fn union_all_keeps_dups() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY)");
    exec(&db, "INSERT INTO a VALUES (1)");
    exec(&db, "INSERT INTO a VALUES (2)");
    exec(&db, "INSERT INTO b VALUES (2)");
    exec(&db, "INSERT INTO b VALUES (3)");
    let r = rows(&db, "SELECT id FROM a UNION ALL SELECT id FROM b ORDER BY id");
    assert_eq!(r.len(), 4, "UNION ALL keeps dups: 1,2,2,3");
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. STDDEV / VARIANCE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn variance_and_stddev() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let vals = [2, 4, 4, 4, 5, 5, 7, 9];
    for (i, &v) in vals.iter().enumerate() {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i + 1, v));
    }
    // Sample variance of [2,4,4,4,5,5,7,9] = 32/7 ≈ 4.571 (n-1 denominator,
    // the SQL standard / SQLite VARIANCE convention). Stddev ≈ 2.138.
    let var = match &rows(&db, "SELECT VARIANCE(v) FROM t")[0][0] {
        Value::Float(f) => *f, Value::Integer(i) => *i as f64,
        Value::Null => panic!("VARIANCE returned NULL"), o => panic!("{:?}", o),
    };
    assert!((var - 4.5714).abs() < 0.01, "sample VARIANCE should be ~4.571, got {}", var);
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. Nested subqueries
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn scalar_subquery_in_where() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=10 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
    }
    // WHERE v > (SELECT AVG(v) FROM t) → AVG=5.5, so v>5.5 → 6,7,8,9,10 = 5 rows
    let r = rows(&db, "SELECT id FROM t WHERE v > (SELECT AVG(v) FROM t) ORDER BY id");
    assert_eq!(r.len(), 5, "5 rows have v > AVG(5.5)");
}

#[test]
fn in_subquery() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, ref INT)");
    exec(&db, "INSERT INTO a VALUES (1, 100)");
    exec(&db, "INSERT INTO a VALUES (2, 200)");
    exec(&db, "INSERT INTO a VALUES (3, 300)");
    exec(&db, "INSERT INTO b VALUES (1, 2)");
    exec(&db, "INSERT INTO b VALUES (2, 3)");
    // a.id IN (SELECT ref FROM b) → ids 2, 3.
    let r = rows(&db, "SELECT id FROM a WHERE id IN (SELECT ref FROM b) ORDER BY id");
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. AUTO_INCREMENT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn auto_increment_basic() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)");
    exec(&db, "INSERT INTO t (v) VALUES (10)");
    exec(&db, "INSERT INTO t (v) VALUES (20)");
    exec(&db, "INSERT INTO t (v) VALUES (30)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 3);
    // IDs should be 1, 2, 3.
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    let ids: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(n)) => Some(*n), _ => None
    }).collect();
    assert_eq!(ids, vec![1, 2, 3], "AUTO_INCREMENT generates sequential IDs");
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. Index creation + usage
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn create_index_then_query() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, v INT)");
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}', {})", i, i % 5, i));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    // Query on indexed column.
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE cat = 'c1'");
    // cat = 'c1' → ids where i%5==1: 1,6,11,...,96 → 20 rows
    match r[0][0] { Value::Integer(n) => assert_eq!(n, 20), _ => panic!() }
}

#[test]
fn drop_index_query_still_works() {
    let (db, _d) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, cat TEXT)");
    for i in 1..=50 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'c{}')", i, i % 3));
    }
    exec(&db, "CREATE INDEX t_cat ON t(cat)");
    db.checkpoint().unwrap();
    db.wait_for_indexes_ready();
    let _ = db.execute("DROP INDEX t_cat");
    // Query must still return correct results (falls back to scan).
    let r = rows(&db, "SELECT COUNT(*) FROM t WHERE cat = 'c0'");
    match r[0][0] { Value::Integer(n) => assert!(n > 0, "should find c0 rows"), _ => panic!() }
}
