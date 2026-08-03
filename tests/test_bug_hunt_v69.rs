//! Bug Hunt v69 — sixteenth round: GROUP BY semantics, empty IN, nested CASE, window edges.

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

// ─────────────────────────────────────────────────────────────────────────
// IN with empty list (edge)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_in_empty_list_errors_or_empty() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // IN () is syntactically invalid in SQL. Should error (or be rejected).
    let res = db.execute("SELECT id FROM t WHERE v IN ()");
    assert!(res.is_err(), "IN () empty list should error");
}

#[test]
fn test_not_in_full_set() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    // NOT IN (10, 20) → nothing
    let r = q(&db, "SELECT id FROM t WHERE v NOT IN (10, 20)");
    assert!(r.is_empty());
}

#[test]
fn test_in_with_zero_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v IN (99, 100)");
    assert!(r.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────
// Nested CASE
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_nested_case() {
    let (db, _d) = db();
    let r = q(
        &db,
        "SELECT CASE WHEN 1 = 1 THEN CASE WHEN 2 = 2 THEN 'a' ELSE 'b' END ELSE 'c' END",
    );
    assert_eq!(r, vec![vec![Value::text("a".into())]]);
}

#[test]
fn test_case_with_null_condition() {
    // WHEN NULL → not matched (NULL is not true), falls through.
    let (db, _d) = db();
    let r = q(&db, "SELECT CASE WHEN NULL THEN 'a' ELSE 'b' END");
    assert_eq!(r, vec![vec![Value::text("b".into())]]);
}

#[test]
fn test_case_in_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)")
        .unwrap();
    // SUM(CASE WHEN v > 10 THEN v ELSE 0 END) = 15+25 = 40
    let r = q(&db, "SELECT SUM(CASE WHEN v > 10 THEN v ELSE 0 END) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(40)]]);
}

#[test]
fn test_case_returns_different_types() {
    // CASE returning Int and Text in different branches.
    let (db, _d) = db();
    let r = q(&db, "SELECT CASE WHEN 1 = 1 THEN 42 ELSE 'x' END");
    // First matched branch → 42
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY column NOT in SELECT (valid SQL)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_col_not_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,30)")
        .unwrap();
    // SELECT SUM(v) grouped by g (g not selected).
    let r = q(&db, "SELECT SUM(v) FROM t GROUP BY g ORDER BY SUM(v)");
    // g1 sum=30, g2 sum=30 → both 30
    assert_eq!(r, vec![vec![Value::Integer(30)], vec![Value::Integer(30)]]);
}

#[test]
fn test_groupby_multiple_keys() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,1,10),(1,1,20),(1,2,30),(2,1,40)")
        .unwrap();
    let r = q(
        &db,
        "SELECT a, b, SUM(v) FROM t GROUP BY a, b ORDER BY a, b",
    );
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(30)],
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(30)],
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(40)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Window function + ORDER BY the window value
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_row_number_order_by_rn() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, score INT)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, ROW_NUMBER() OVER (ORDER BY score DESC) AS rn FROM s ORDER BY rn",
    );
    // desc: 30(rn1),20(rn2),10(rn3) → ids 1,3,2
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(3), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(3)],
        ]
    );
}

#[test]
fn test_rank_order_by_partition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE s(id INT PRIMARY KEY, g INT, v INT)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1,1,10),(2,1,20),(3,2,30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT id, RANK() OVER (PARTITION BY g ORDER BY v DESC) AS rk FROM s ORDER BY id",
    );
    // g1: v20→rk1, v10→rk2 ; g2: v30→rk1
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(3), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Arithmetic with mixed int/float results
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_int_plus_float_is_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 2 + 0.5");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9),
        _ => panic!("2 + 0.5 should be Float 2.5, got {:?}", r),
    }
}

#[test]
fn test_float_times_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1.5 * 4");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 6.0).abs() < 1e-9),
        _ => panic!("{:?}", r),
    }
}

#[test]
fn test_int_divided_by_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 10 / 4.0");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9),
        _ => panic!("{:?}", r),
    }
}

#[test]
fn test_float_divided_by_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 7.5 / 3");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 2.5).abs() < 1e-9),
        _ => panic!("{:?}", r),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT in count with multiple args
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_distinct_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'a'),(4,'c')")
        .unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT s) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_count_distinct_null_excluded() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,1),(3,NULL),(4,2)")
        .unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// String: trim variants edge
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_trim_only_whitespace() {
    let (db, _d) = db();
    let r = q(&db, "SELECT TRIM('   ')");
    assert_eq!(r, vec![vec![Value::text("".into())]]);
}

#[test]
fn test_ltrim_rtrim_combined() {
    let (db, _d) = db();
    let r = q(&db, "SELECT LTRIM(RTRIM('  x  '))");
    assert_eq!(r, vec![vec![Value::text("x".into())]]);
}

#[test]
fn test_length_of_trimmed() {
    let (db, _d) = db();
    let r = q(&db, "SELECT LENGTH(TRIM('  abc  '))");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE with CASE expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_with_case() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, bucket TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5,''),(2,15,''),(3,25,'')")
        .unwrap();
    db.execute(
        "UPDATE t SET bucket = CASE WHEN v < 10 THEN 'low' WHEN v < 20 THEN 'mid' ELSE 'high' END",
    )
    .unwrap();
    let r = q(&db, "SELECT bucket FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::text("low".into())],
            vec![Value::text("mid".into())],
            vec![Value::text("high".into())],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// LIMIT 1 (common pattern)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_limit_one() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,20)")
        .unwrap();
    // LIMIT 1 with ORDER BY → smallest v's id
    let r = q(&db, "SELECT id FROM t ORDER BY v LIMIT 1");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_limit_one_no_order() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10)").unwrap();
    // LIMIT 1 without ORDER BY → any single row.
    let r = q(&db, "SELECT id FROM t LIMIT 1");
    assert_eq!(r.len(), 1);
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in FROM (derived table)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_derived_table_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT s, COUNT(*) FROM (SELECT g, SUM(v) AS s FROM t GROUP BY g) sub GROUP BY s ORDER BY s",
    );
    // inner: g1→30, g2→30. outer GROUP BY s: one group s=30 count 1... wait, 2 rows both s=30.
    assert_eq!(r, vec![vec![Value::Integer(30), Value::Integer(2)]]);
}

#[test]
fn test_derived_table_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT v FROM (SELECT v FROM t WHERE v > 15) sub ORDER BY v",
    );
    assert_eq!(r, vec![vec![Value::Integer(20)], vec![Value::Integer(30)]]);
}
