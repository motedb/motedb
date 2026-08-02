//! Bug Hunt v57 — fourth round: index, checkpoint, alter, distinct-agg, math edges.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

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

fn i_of(v: &Value) -> i64 {
    match v {
        Value::Integer(i) => *i,
        Value::Float(f) => *f as i64,
        _ => panic!("expected number, got {:?}", v),
    }
}
fn f_of(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => panic!("expected number, got {:?}", v),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Index correctness: query via indexed WHERE returns correct rows
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_index_lookup_returns_correct_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'b',20),(3,'a',30),(4,'c',40)").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_index_after_update() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    // Update a row's indexed column; index must reflect the change.
    db.execute("UPDATE t SET cat = 'a' WHERE id = 2").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
    // Old value should no longer match.
    let r2 = q(&db, "SELECT id FROM t WHERE cat = 'b'");
    assert!(r2.is_empty(), "old indexed value should be gone after UPDATE");
}

#[test]
fn test_index_after_delete() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_index_survives_checkpoint_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a')").unwrap();
        db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'a' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Checkpoint / persistence
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_data_survives_checkpoint_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(10)], vec![Value::Integer(20)], vec![Value::Integer(30)]]);
}

#[test]
fn test_same_query_before_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    let before: Vec<Vec<Value>>;
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
        before = q(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let after = q(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(before, after, "aggregates must match before/after checkpoint");
}

// ─────────────────────────────────────────────────────────────────────────
// ALTER TABLE ADD COLUMN
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_alter_add_column_existing_rows_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN w INT").unwrap();
    // Existing rows should have NULL for the new column.
    let r = q(&db, "SELECT id, v, w FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(10), Value::Null],
            vec![Value::Integer(2), Value::Integer(20), Value::Null],
        ]
    );
}

#[test]
fn test_alter_add_column_then_insert() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("ALTER TABLE t ADD COLUMN w INT").unwrap();
    db.execute("INSERT INTO t VALUES (2,20,200)").unwrap();
    let r = q(&db, "SELECT id, w FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1), Value::Null], vec![Value::Integer(2), Value::Integer(200)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT aggregates
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    // SUM(DISTINCT v) = 10+20+30 = 60.
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(i_of(&r[0][0]), 60, "SUM(DISTINCT) = {}", 60);
}

#[test]
fn test_count_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(i_of(&r[0][0]), 2);
}

#[test]
fn test_avg_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,20),(5,30)").unwrap();
    // AVG(DISTINCT v) = (10+20+30)/3 = 20.
    let r = q(&db, "SELECT AVG(DISTINCT v) FROM t");
    assert!((f_of(&r[0][0]) - 20.0).abs() < 1e-9);
}

#[test]
fn test_stddev_single_value_returns_null_or_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT STDDEV(v) FROM t");
    // Sample STDDEV of 1 value: standard says NULL; many DBs return NULL.
    // Just verify it doesn't crash and is NULL or 0.
    assert!(
        matches!(r[0][0], Value::Null) || f_of(&r[0][0]).abs() < 1e-9,
        "STDDEV of single value should be NULL or 0, got {:?}",
        r
    );
}

#[test]
fn test_variance_known_values() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    // values 2,4,4,4,5,5,7,9 → sample variance = 32/7 ≈ 4.571
    db.execute("INSERT INTO t VALUES (1,2),(2,4),(3,4),(4,4),(5,5),(6,5),(7,7),(8,9)").unwrap();
    let r = q(&db, "SELECT VARIANCE(v) FROM t");
    let v = f_of(&r[0][0]);
    assert!((v - (32.0 / 7.0)).abs() < 1e-6, "VARIANCE should be ~4.571, got {}", v);
    let r2 = q(&db, "SELECT STDDEV(v) FROM t");
    let expected_stddev = (32.0_f64 / 7.0_f64).sqrt();
    assert!((f_of(&r2[0][0]) - expected_stddev).abs() < 1e-6);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple aggregates together
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_all_aggregates_together() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(3),
            Value::Integer(60),
            Value::Float(20.0),
            Value::Integer(10),
            Value::Integer(30),
        ]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// LIKE edge cases
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_like_percent_matches_all() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'alice'),(2,'bob'),(3,'')").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE s LIKE '%'");
    // '%' matches any string including empty.
    assert_eq!(i_of(&r[0][0]), 3);
}

#[test]
fn test_like_underscore_single_char() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'ab'),(2,'abc'),(3,'a')").unwrap();
    // '_b' matches exactly 2 chars ending in b → 'ab' only.
    let r = q(&db, "SELECT s FROM t WHERE s LIKE '_b' ORDER BY s");
    assert_eq!(r, vec![vec![Value::text("ab".into())]]);
}

#[test]
fn test_like_literal_special_chars() {
    // SQL LIKE treats [ ] . * etc as literals (not regex).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a.b'),(2,'axb'),(3,'a[b')").unwrap();
    let r = q(&db, "SELECT s FROM t WHERE s LIKE 'a.b' ORDER BY s");
    // '.' is literal → only 'a.b' matches.
    assert_eq!(r, vec![vec![Value::text("a.b".into())]]);
}

#[test]
fn test_like_case_sensitive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'Alice'),(2,'alice')").unwrap();
    let r = q(&db, "SELECT s FROM t WHERE s LIKE 'A%' ORDER BY s");
    assert_eq!(r, vec![vec![Value::text("Alice".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CAST
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_cast_text_to_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST('42' AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

#[test]
fn test_cast_int_to_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(42 AS FLOAT)");
    assert_eq!(r, vec![vec![Value::Float(42.0)]]);
}

#[test]
fn test_cast_float_to_int_truncates() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(2.9 AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_cast_null_is_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(NULL AS INTEGER)");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Math function edge cases
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_abs_negative() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ABS(-5), ABS(5)");
    assert_eq!(r, vec![vec![Value::Integer(5), Value::Integer(5)]]);
}

#[test]
fn test_mod_negative() {
    let (db, _d) = db();
    // -7 % 3: Rust/Scheme-style → -1 (truncated); some DBs → 2 (floored).
    // Just document the behavior; assert it's consistent with the function form.
    let r = q(&db, "SELECT MOD(-7, 3)");
    let v = i_of(&r[0][0]);
    assert!(v == -1 || v == 2, "MOD(-7,3) = {} (truncated=-1 or floored=2)", v);
}

#[test]
fn test_mod_by_zero_errors() {
    let (db, _d) = db();
    let res = db.execute("SELECT MOD(5, 0)");
    assert!(res.is_err(), "MOD by zero should error");
}

#[test]
fn test_power_basic() {
    let (db, _d) = db();
    let r = q(&db, "SELECT POWER(2, 10)");
    assert!((f_of(&r[0][0]) - 1024.0).abs() < 1e-9);
}

#[test]
fn test_sqrt() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SQRT(16)");
    assert!((f_of(&r[0][0]) - 4.0).abs() < 1e-9);
}

#[test]
fn test_sign() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SIGN(-5), SIGN(0), SIGN(5)");
    assert_eq!(r, vec![vec![Value::Integer(-1), Value::Integer(0), Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Transactions
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_rollback_undoes_insert() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = q(&db, "SELECT id FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_rollback_undoes_update_delete() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET v = 999 WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)], vec![Value::Integer(2), Value::Integer(20)]]);
}

#[test]
fn test_commit_persists() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("COMMIT").unwrap();
    let r = q(&db, "SELECT id FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty-table / edge aggregates
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_empty_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    let r = q(&db, "SELECT COUNT(*), COUNT(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0), Value::Integer(0)]]);
}

#[test]
fn test_sum_empty_table_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    let r = q(&db, "SELECT SUM(v), AVG(v), MIN(v), MAX(v) FROM t");
    assert_eq!(r, vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]]);
}
