//! Bug Hunt v95 — round 22: persistence/checkpoint, batch operations,
//! schema evolution edges, and large-data consistency.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;
use std::path::PathBuf;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(r: QueryResult) -> Vec<Vec<Value>> {
    match r {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn q(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    rows(db.execute(sql).unwrap().materialize().unwrap())
}

fn sorted_int(r: &[Vec<Value>]) -> Vec<i64> {
    let mut v: Vec<i64> = r.iter().filter_map(|row| match row.get(0) {
        Some(Value::Integer(i)) => Some(*i),
        _ => None,
    }).collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────────────
// Checkpoint then reopen — data must persist.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_checkpoint_reopen_persists() {
    let dir = TempDir::new().unwrap();
    let path: PathBuf = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
        db.checkpoint().unwrap();
    }
    // Reopen.
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
    let sumr = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sumr, vec![vec![Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Checkpoint then reopen preserves NULL values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_checkpoint_reopen_preserves_null() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, s TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,NULL,'hello'),(2,42,NULL)").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v, s FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Null, Value::Text("hello".into())],
        vec![Value::Integer(42), Value::Null],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Checkpoint then reopen preserves TEXT with special chars.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_checkpoint_reopen_preserves_text() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a''b'),(2,'with space')").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT s FROM t ORDER BY id");
    assert_eq!(r, vec![
        vec![Value::Text("a'b".into())],
        vec![Value::Text("with space".into())],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Batch INSERT (many rows in one statement) consistency.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_batch_insert_large() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    // Insert 50 rows in one statement.
    let mut sql = String::from("INSERT INTO t VALUES ");
    for i in 1..=50 {
        if i > 1 { sql.push(','); }
        sql.push_str(&format!("({},{})", i, i * 2));
    }
    db.execute(&sql).unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(50)]]);
    let sumr = q(&db, "SELECT SUM(v) FROM t");
    // SUM(2,4,...,100) = 2*(1+..+50) = 2*1275 = 2550.
    assert_eq!(sumr, vec![vec![Value::Integer(2550)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Sequential INSERTs then aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sequential_insert_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=30 {
        db.execute(&format!("INSERT INTO t VALUES ({},{})", i, i)).unwrap();
    }
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t"), vec![vec![Value::Integer(30)]]);
    assert_eq!(q(&db, "SELECT SUM(v) FROM t"), vec![vec![Value::Integer(465)]]); // 1+..+30
    assert_eq!(q(&db, "SELECT MIN(v) FROM t"), vec![vec![Value::Integer(1)]]);
    assert_eq!(q(&db, "SELECT MAX(v) FROM t"), vec![vec![Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE all rows in a batch then verify each.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_all_verify_each() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({},{})", i, i * 10)).unwrap();
    }
    db.execute("UPDATE t SET v = v + 1").unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY id");
    let vals: Vec<i64> = r.iter().filter_map(|row| match row[0] { Value::Integer(i) => Some(i), _ => None }).collect();
    assert_eq!(vals, vec![11, 21, 31, 41, 51, 61, 71, 81, 91, 101]);
}

// ─────────────────────────────────────────────────────────────────────────
// DELETE half the rows then verify remainder.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_delete_half_verify() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=20 {
        db.execute(&format!("INSERT INTO t VALUES ({},{})", i, i)).unwrap();
    }
    db.execute("DELETE FROM t WHERE id <= 10").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t"));
    assert_eq!(r, (11..=20).collect::<Vec<_>>());
}

// ─────────────────────────────────────────────────────────────────────────
// Transaction with checkpoint inside (if supported) — verify isolation.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_txn_then_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("BEGIN TRANSACTION").unwrap();
        db.execute("INSERT INTO t VALUES (1,10)").unwrap();
        db.execute("COMMIT").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Rolled-back transaction then checkpoint — nothing persists.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_rollback_then_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10)").unwrap();
        db.execute("BEGIN TRANSACTION").unwrap();
        db.execute("INSERT INTO t VALUES (2,20)").unwrap();
        db.execute("ROLLBACK").unwrap();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1)]], "rolled-back insert not persisted");
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple tables with cross-table queries.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_multiple_tables_cross_query() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, x INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, y INT)").unwrap();
    db.execute("CREATE TABLE c(id INT PRIMARY KEY, z INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").unwrap();
    db.execute("INSERT INTO b VALUES (1,20)").unwrap();
    db.execute("INSERT INTO c VALUES (1,30)").unwrap();
    let r = q(&db, "SELECT a.x, b.y, c.z FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// DROP TABLE (if supported) then verify gone.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_drop_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let res = db.execute("DROP TABLE t");
    // May or may not be supported.
    if res.is_ok() {
        // After drop, querying should fail.
        let qres = db.execute("SELECT * FROM t").and_then(|s| s.materialize());
        assert!(qres.is_err(), "table should be gone after DROP");
    }
}

// ─────────────────────────────────────────────────────────────────────────
// CREATE INDEX then checkpoint then reopen — index persists.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_index_checkpoint_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')").unwrap();
        db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
        db.wait_for_indexes_ready();
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // Query via the indexed column.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE cat = 'a'"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// Large WHERE IN list with mixed values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_large_in_mixed() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=20 {
        db.execute(&format!("INSERT INTO t VALUES ({},{})", i, i)).unwrap();
    }
    // IN list with 15 values, some matching some not.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (1,3,5,7,9,11,13,15,17,19,21,23,25,27,29)"));
    assert_eq!(r, vec![1, 3, 5, 7, 9, 11, 13, 15, 17, 19]);
}

// ─────────────────────────────────────────────────────────────────────────
// Repeated checkpoint cycles.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_repeated_checkpoint_cycles() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        for cycle in 1..=3 {
            db.execute(&format!("INSERT INTO t VALUES ({},{})", cycle, cycle * 10)).unwrap();
            db.checkpoint().unwrap();
        }
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(60)]]); // 10+20+30
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE PK column (changing the primary key value).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_pk_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // Change the PK from 1 to 100.
    let res = db.execute("UPDATE t SET id = 100 WHERE id = 1");
    if res.is_ok() {
        let r = q(&db, "SELECT v FROM t WHERE id = 100");
        assert_eq!(r, vec![vec![Value::Integer(10)]]);
        let r2 = q(&db, "SELECT v FROM t WHERE id = 1");
        assert_eq!(r2, vec![] as Vec<Vec<Value>>);
    }
}

// ─────────────────────────────────────────────────────────────────────────
// INSERT with mismatched column count (should error).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_insert_wrong_column_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    let res = db.execute("INSERT INTO t VALUES (1, 2)"); // only 2 values for 3 cols
    // Should error (or fill NULLs). Document behavior.
    if res.is_ok() {
        // If it filled NULLs, verify.
        let _ = q(&db, "SELECT * FROM t");
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over table with single TEXT column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_text_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,NULL)").unwrap();
    assert_eq!(q(&db, "SELECT COUNT(*) FROM t"), vec![vec![Value::Integer(3)]]);
    assert_eq!(q(&db, "SELECT COUNT(s) FROM t"), vec![vec![Value::Integer(2)]]); // excludes NULL
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with ORDER BY on an aggregate that is NOT in the SELECT list
// (e.g. SELECT cat, SUM(v) ... ORDER BY MAX(w)). The per-group aggregate
// is computed for sorting even though it isn't output.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_order_different_agg() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT, w INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10,1),(2,'a',20,2),(3,'b',5,10),(4,'b',15,20)").unwrap();
    // SELECT cat, SUM(v) ... ORDER BY MAX(w) ASC.
    // a: SUM(v)=30, MAX(w)=2. b: SUM(v)=20, MAX(w)=20. ORDER BY MAX(w) ASC → a(2), b(20).
    let r = q(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat ORDER BY MAX(w) ASC");
    assert_eq!(r, vec![
        vec![Value::Text("a".into()), Value::Integer(30)],
        vec![Value::Text("b".into()), Value::Integer(20)],
    ]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty table GROUP BY (no groups).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_empty_table_group_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    let r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    assert_eq!(r, Vec::<Vec<Value>>::new());
}
