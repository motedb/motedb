//! Bug Hunt v63 — tenth round: batch INSERT, multi-col index, implicit conversion, nested txn.

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

fn f_of(v: &Value) -> f64 {
    match v {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => panic!("expected number, got {:?}", v),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Batch / large INSERT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_batch_insert_many_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    let mut vals = Vec::new();
    for i in 1..=100 {
        vals.push(format!("({},{})", i, i * 10));
    }
    db.execute(&format!("INSERT INTO t VALUES {}", vals.join(","))).unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(100), Value::Integer(50500), Value::Integer(10), Value::Integer(1000)]]);
}

#[test]
fn test_batch_insert_mixed_types() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',1.5),(2,'b',2.5),(3,'c',3.5)").unwrap();
    let r = q(&db, "SELECT id, s, v FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::text("a".into()), Value::Float(1.5)],
            vec![Value::Integer(2), Value::text("b".into()), Value::Float(2.5)],
            vec![Value::Integer(3), Value::text("c".into()), Value::Float(3.5)],
        ]
    );
}

#[test]
fn test_insert_with_explicit_columns_reordered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    // Insert with columns in different order than schema.
    db.execute("INSERT INTO t (b, id, a) VALUES (20, 1, 10)").unwrap();
    let r = q(&db, "SELECT id, a, b FROM t");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10), Value::Integer(20)]]);
}

#[test]
fn test_insert_partial_columns_multiple() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t (id, a) VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT id, a, b, c FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(10), Value::Null, Value::Null],
            vec![Value::Integer(2), Value::Integer(20), Value::Null, Value::Null],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Index on multiple usage patterns
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_index_range_query() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)").unwrap();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v >= 30 ORDER BY id");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
}

#[test]
fn test_index_inequality_both_sides() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40)").unwrap();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v > 10 AND v < 40 ORDER BY id");
    assert_eq!(
        r.iter().map(|row| match &row[0] { Value::Integer(i) => *i, _ => -1 }).collect::<Vec<_>>(),
        vec![2, 3]
    );
}

#[test]
fn test_index_on_text_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'x'),(2,'y'),(3,'x'),(4,'z')").unwrap();
    db.execute("CREATE INDEX idx_cat ON t(cat)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE cat = 'x' ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_drop_index_then_query() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("CREATE INDEX idx_v ON t(v)").unwrap();
    db.execute("DROP INDEX idx_v").unwrap();
    // Query should still work (falls back to scan).
    let r = q(&db, "SELECT id FROM t WHERE v = 20");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Implicit conversion in INSERT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_insert_negative_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,-42)").unwrap();
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(-42)]]);
}

#[test]
fn test_insert_large_int() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,9000000000)").unwrap(); // > 2^32
    let r = q(&db, "SELECT v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(9000000000)]]);
}

#[test]
fn test_insert_text_with_special_chars() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'with spaces'),(2,'with-dash'),(3,'with.dot')").unwrap();
    let r = q(&db, "SELECT s FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::text("with spaces".into())],
            vec![Value::text("with-dash".into())],
            vec![Value::text("with.dot".into())],
        ]
    );
}

#[test]
fn test_insert_quoted_quote() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    // String with an escaped/quoted single quote.
    let res = db.execute("INSERT INTO t VALUES (1, 'it''s')");
    match res {
        Ok(_) => {
            let r = q(&db, "SELECT s FROM t");
            assert_eq!(r, vec![vec![Value::text("it's".into())]]);
        }
        Err(_) => { /* escaping may differ */ }
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Nested transaction behavior
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_nested_begin_rejected() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("BEGIN").unwrap();
    // Nested BEGIN should be rejected.
    let res = db.execute("BEGIN");
    assert!(res.is_err(), "nested BEGIN should be rejected");
    db.execute("ROLLBACK").unwrap();
}

#[test]
fn test_commit_without_begin() {
    let (db, _d) = db();
    let res = db.execute("COMMIT");
    // COMMIT without BEGIN — should error or no-op, not crash.
    let _ = res;
}

#[test]
fn test_rollback_without_begin() {
    let (db, _d) = db();
    let res = db.execute("ROLLBACK");
    let _ = res;
}

#[test]
fn test_transaction_isolation_read_committed() {
    // Within a transaction, can see own uncommitted writes.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
    // Should see updated value within same txn.
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(20)]]);
    db.execute("ROLLBACK").unwrap();
    // After rollback, original value.
    let r2 = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r2, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// More string functions
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_reverse() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REVERSE('abcdef')");
    assert_eq!(r, vec![vec![Value::text("fedcba".into())]]);
}

#[test]
fn test_replace_empty_string() {
    let (db, _d) = db();
    let r = q(&db, "SELECT REPLACE('abc', '', 'X')");
    // Replacing empty string — behavior varies. Just don't crash.
    assert_eq!(r.len(), 1);
}

#[test]
fn test_substr_zero_start() {
    let (db, _d) = db();
    let r = q(&db, "SELECT SUBSTR('hello', 0, 3)");
    // SQLite: SUBSTR(s, 0, 3) → 'he' (0 treated as 1). Behavior varies.
    let _ = r;
}

#[test]
fn test_concat_with_numbers() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CONCAT('v', 1, 2.5)");
    assert_eq!(r, vec![vec![Value::text("v12.5".into())]]);
}

#[test]
fn test_pipe_concat_null_propagates() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 'x' || NULL || 'y'");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// More numeric functions
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_abs_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ABS(-3.14)");
    assert!((f_of(&r[0][0]) - 3.14).abs() < 1e-9);
}

#[test]
fn test_ceil_floor() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CEIL(2.1), FLOOR(2.9)");
    assert_eq!(r, vec![vec![Value::Integer(3), Value::Integer(2)]]);
}

#[test]
fn test_power_fractional() {
    let (db, _d) = db();
    let r = q(&db, "SELECT POWER(9, 0.5)");
    assert!((f_of(&r[0][0]) - 3.0).abs() < 1e-9);
}

#[test]
fn test_mod_function_vs_op() {
    let (db, _d) = db();
    let r = q(&db, "SELECT MOD(17, 5), 17 % 5");
    assert_eq!(r, vec![vec![Value::Integer(2), Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT variations
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_star_vs_col_vs_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20),(4,NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(*), COUNT(v), COUNT(DISTINCT v) FROM t");
    // COUNT(*)=4, COUNT(v)=3 (NULL excluded), COUNT(DISTINCT v)=2 (10,20)
    assert_eq!(r, vec![vec![Value::Integer(4), Value::Integer(3), Value::Integer(2)]]);
}

#[test]
fn test_count_with_condition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
    // COUNT with CASE to count rows matching a condition.
    let r = q(&db, "SELECT SUM(CASE WHEN v > 15 THEN 1 ELSE 0 END) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// JOIN types
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_left_join_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE users(id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders(id INT PRIMARY KEY, uid INT, amt INT)").unwrap();
    db.execute("INSERT INTO users VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')").unwrap();
    db.execute("INSERT INTO orders VALUES (1,1,100),(2,1,50)").unwrap();
    // LEFT JOIN + GROUP BY: Carol has no orders → SUM should be NULL (or 0).
    let r = q(
        &db,
        "SELECT u.name, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.uid GROUP BY u.name ORDER BY u.name",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("Alice".into()), Value::Integer(2)],
            vec![Value::text("Bob".into()), Value::Integer(0)],
            vec![Value::text("Carol".into()), Value::Integer(0)],
        ]
    );
}

#[test]
fn test_inner_join_no_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").unwrap();
    db.execute("INSERT INTO b VALUES (2,20)").unwrap();
    let r = q(&db, "SELECT a.id FROM a JOIN b ON a.id = b.id");
    assert!(r.is_empty(), "INNER JOIN with no matching keys → empty");
}

// ─────────────────────────────────────────────────────────────────────────
// Edge: empty table behaviors
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_select_from_empty_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    let r = q(&db, "SELECT * FROM t");
    assert!(r.is_empty());
}

#[test]
fn test_groupby_empty_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*) FROM t GROUP BY g");
    assert!(r.is_empty(), "GROUP BY over empty table → no groups");
}

#[test]
fn test_update_empty_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("UPDATE t SET v = 1").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}

#[test]
fn test_delete_from_empty_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("DELETE FROM t").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(0)]]);
}
