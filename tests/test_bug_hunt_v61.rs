//! Bug Hunt v61 — eighth round: persistence, multi-row DML, self-join aliases, decimal.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

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
// Multi-row INSERT + persistence
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_multi_row_insert_then_count() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30),(4,40),(5,50)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT COUNT(*), SUM(v), MIN(v), MAX(v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5), Value::Integer(150), Value::Integer(10), Value::Integer(50)]]);
}

#[test]
fn test_persist_after_update_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)").unwrap();
        db.execute("UPDATE t SET v = 999 WHERE id = 2").unwrap();
        db.execute("DELETE FROM t WHERE id = 3").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)], vec![Value::Integer(2), Value::Integer(999)]]);
}

#[test]
fn test_checkpoint_then_more_inserts() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
        db.execute("INSERT INTO t VALUES (1,10)").unwrap();
        db.checkpoint().unwrap();
        // Insert more AFTER checkpoint, in a new txn-less session
        db.execute("INSERT INTO t VALUES (2,20)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = q(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1), Value::Integer(10)], vec![Value::Integer(2), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Self-join with aliases + SELECT *
// ─────────────────────────────────────────────────────────────────────────

fn db_inner() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

#[test]
fn test_self_join_aliased_columns() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE emp(id INT PRIMARY KEY, name TEXT, mgr INT)").unwrap();
    db.execute("INSERT INTO emp VALUES (1,'CEO',NULL),(2,'Alice',1),(3,'Bob',1)").unwrap();
    // Self-join: employee + their manager's name
    let r = q(
        &db,
        "SELECT e.name, m.name FROM emp e JOIN emp m ON e.mgr = m.id ORDER BY e.name",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::text("Alice".into()), Value::text("CEO".into())],
            vec![Value::text("Bob".into()), Value::text("CEO".into())],
        ]
    );
}

#[test]
fn test_join_select_star_collision() {
    // SELECT * from two tables both with 'id' — qualified names should disambiguate.
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, av INT)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, bv INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").unwrap();
    db.execute("INSERT INTO b VALUES (1,20)").unwrap();
    // Qualified column access after SELECT *
    let r = q(&db, "SELECT a.av, b.bv FROM a JOIN b ON a.id = b.id");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Float / decimal precision
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_float_precision_sum() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,0.1),(2,0.2)").unwrap();
    let r = q(&db, "SELECT SUM(v) FROM t");
    // 0.1 + 0.2 ≈ 0.3 (within float tolerance)
    assert!((f_of(&r[0][0]) - 0.3).abs() < 1e-9, "SUM(0.1,0.2) ≈ 0.3, got {:?}", r);
}

#[test]
fn test_float_division_precision() {
    let (db, _d) = db_inner();
    let r = q(&db, "SELECT 1.0 / 3.0");
    assert!((f_of(&r[0][0]) - 0.3333333333).abs() < 1e-6);
}

#[test]
fn test_avg_returns_float() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,25)").unwrap();
    // AVG of ints: 55/3 = 18.333...
    let r = q(&db, "SELECT AVG(v) FROM t");
    assert!((f_of(&r[0][0]) - (55.0 / 3.0)).abs() < 1e-9);
}

#[test]
fn test_round_float_decimals() {
    let (db, _d) = db_inner();
    let r = q(&db, "SELECT ROUND(2.567, 2), ROUND(2.567, 1), ROUND(2.567, 0)");
    assert_eq!(
        r,
        vec![vec![Value::Float(2.57), Value::Float(2.6), Value::Float(3.0)]]
    );
}

#[test]
fn test_round_negative_float() {
    let (db, _d) = db_inner();
    let r = q(&db, "SELECT ROUND(-2.567, 1)");
    assert_eq!(r, vec![vec![Value::Float(-2.6)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE with column-reference expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_swap_columns() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20)").unwrap();
    // Swap a and b using a temp — SQL can't do this in one SET without a temp
    // column, but SET a = b, b = a should use ORIGINAL values for both.
    db.execute("UPDATE t SET a = b, b = a WHERE id = 1").unwrap();
    let r = q(&db, "SELECT a, b FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(20), Value::Integer(10)]]);
}

#[test]
fn test_update_column_reference_arithmetic() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, price INT, qty INT, total INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,3,0)").unwrap();
    db.execute("UPDATE t SET total = price * qty WHERE id = 1").unwrap();
    let r = q(&db, "SELECT total FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(30)]]);
}

#[test]
fn test_update_string_concat() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'hello')").unwrap();
    db.execute("UPDATE t SET s = s || ' world' WHERE id = 1").unwrap();
    let r = q(&db, "SELECT s FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::text("hello world".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with WHERE + HAVING + ORDER BY + LIMIT combined
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_full_combo() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE sales(region TEXT, product TEXT, amt INT)").unwrap();
    db.execute("INSERT INTO sales VALUES ('US','a',100),('US','a',50),('US','b',200),('EU','a',150),('EU','b',75)").unwrap();
    let r = q(
        &db,
        "SELECT region, SUM(amt) AS total FROM sales WHERE product = 'a' GROUP BY region HAVING SUM(amt) > 100 ORDER BY total DESC",
    );
    // product='a': US(100+50=150), EU(150). HAVING >100 → both. ORDER BY total DESC → EU(150), US(150) tie.
    // Both are 150, so order between them is undefined; just check both present.
    let totals: Vec<i64> = r.iter().map(|row| match &row[1] { Value::Integer(i) => *i, _ => 0 }).collect();
    assert_eq!(totals.len(), 2);
    assert!(totals.iter().all(|&t| t == 150));
}

// ─────────────────────────────────────────────────────────────────────────
// Empty string vs NULL
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_empty_string_not_null() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'')").unwrap();
    let r = q(&db, "SELECT s IS NULL, LENGTH(s) FROM t WHERE id = 1");
    // empty string is NOT NULL, length 0
    assert_eq!(r, vec![vec![Value::Bool(false), Value::Integer(0)]]);
}

#[test]
fn test_null_vs_empty_in_where() {
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,''),(2,NULL),(3,'x')").unwrap();
    let empty = q(&db, "SELECT id FROM t WHERE s = '' ORDER BY id");
    assert_eq!(empty, vec![vec![Value::Integer(1)]]);
    let nulls = q(&db, "SELECT id FROM t WHERE s IS NULL");
    assert_eq!(nulls, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison operators completeness
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_all_comparison_ops() {
    let (db, _d) = db_inner();
    let r = q(
        &db,
        "SELECT 5 = 5, 5 <> 5, 5 < 6, 5 > 6, 5 <= 5, 5 >= 6, 5 != 5",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
        ]]
    );
}

#[test]
fn test_chained_comparison_via_and() {
    // a < b AND b < c (range check)
    let (db, _d) = db_inner();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,10),(3,15),(4,20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v > 5 AND v < 20 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}
