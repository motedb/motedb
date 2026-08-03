//! Bug Hunt v74 — twenty-first round: identifier case, qualified names, misc edges.

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
// Identifier case sensitivity
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_column_name_case_insensitive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(MyCol INT)").unwrap();
    db.execute("INSERT INTO t VALUES (42)").unwrap();
    // Refer to column with different case.
    let r = q(&db, "SELECT MYCOL FROM t");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

#[test]
fn test_table_name_case_sensitive_or_insensitive() {
    let (db, _d) = db();
    db.execute("CREATE TABLE MyTable(id INT PRIMARY KEY)")
        .unwrap();
    db.execute("INSERT INTO MyTable VALUES (1)").unwrap();
    // Document: table name case. Try same case first (must work).
    let r = q(&db, "SELECT id FROM MyTable");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Qualified column names (table.column)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_qualified_column_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT t.v FROM t");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

#[test]
fn test_qualified_column_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = q(&db, "SELECT id FROM t WHERE t.v = 20");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_qualified_column_with_alias() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r = q(&db, "SELECT x.v FROM t x");
    assert_eq!(r, vec![vec![Value::Integer(10)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Column name with table prefix in JOIN
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_join_qualified_no_ambiguity() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO a VALUES (1,10)").unwrap();
    db.execute("INSERT INTO b VALUES (1,20)").unwrap();
    // Both have 'v' — must qualify.
    let r = q(&db, "SELECT a.v, b.v FROM a JOIN b ON a.id = b.id");
    assert_eq!(r, vec![vec![Value::Integer(10), Value::Integer(20)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT * expansion
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_select_star_column_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2,3)").unwrap();
    let r = q(&db, "SELECT * FROM t");
    assert_eq!(r[0].len(), 3, "SELECT * should return all 3 columns");
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3)
        ]]
    );
}

#[test]
fn test_select_star_order_matches_schema() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(first INT, second INT, third INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (10,20,30)").unwrap();
    let r = q(&db, "SELECT * FROM t");
    // Order should match schema definition order.
    assert_eq!(
        r[0],
        vec![Value::Integer(10), Value::Integer(20), Value::Integer(30)]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate of constant
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_sum_constant() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // SUM(5) over 3 rows = 15.
    let r = q(&db, "SELECT SUM(5) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(15)]]);
}

#[test]
fn test_count_constant() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    // COUNT(5) counts non-null → 3.
    let r = q(&db, "SELECT COUNT(5) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

#[test]
fn test_max_constant() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT MAX(5) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(5)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY with aggregate of constant
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_count_constant() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(1),(2)").unwrap();
    let r = q(&db, "SELECT g, COUNT(1) FROM t GROUP BY g ORDER BY g");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(1)]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Negative number as literal in various contexts
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_negative_in_select() {
    let (db, _d) = db();
    let r = q(&db, "SELECT -1, -2.5, -100");
    assert_eq!(
        r,
        vec![vec![
            Value::Integer(-1),
            Value::Float(-2.5),
            Value::Integer(-100)
        ]]
    );
}

#[test]
fn test_negative_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-10),(2,5),(3,-20)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE v < 0 ORDER BY id");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(3)]]);
}

#[test]
fn test_negative_in_order_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-10),(2,5),(3,-20)")
        .unwrap();
    let r = q(&db, "SELECT v FROM t ORDER BY v");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(-20)],
            vec![Value::Integer(-10)],
            vec![Value::Integer(5)]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// String comparison ordering
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_string_ordering_numbers_as_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'10'),(2,'9'),(3,'100')")
        .unwrap();
    // Lexical order: '10' < '100' < '9'.
    let r = q(&db, "SELECT s FROM t ORDER BY s");
    assert_eq!(
        r,
        vec![
            vec![Value::text("10".into())],
            vec![Value::text("100".into())],
            vec![Value::text("9".into())]
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT(*) over JOIN
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_count_star_join() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO b VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_count_star_left_join() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE b(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO b VALUES (1)").unwrap();
    // LEFT JOIN: all of a (3 rows), b NULL-filled for 2,3.
    let r = q(&db, "SELECT COUNT(*) FROM a LEFT JOIN b ON a.id = b.id");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE returning affected count matches actual change
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_update_count_matches_verify() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,10),(3,20)")
        .unwrap();
    let res = db.execute("UPDATE t SET v = 99 WHERE v = 10").unwrap();
    let n = match res.materialize().unwrap() {
        QueryResult::Modification { affected_rows } => affected_rows,
        _ => 0,
    };
    assert_eq!(n, 2);
    // Verify actual change.
    let changed = q(&db, "SELECT COUNT(*) FROM t WHERE v = 99");
    assert_eq!(changed, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Empty string in WHERE equality
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_empty_string_equality() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,''),(2,'x'),(3,NULL)")
        .unwrap();
    let r = q(&db, "SELECT id FROM t WHERE s = ''");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple ORDER BY columns same direction
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_order_by_two_cols_asc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,2),(1,1),(2,1),(1,3)")
        .unwrap();
    let r = q(&db, "SELECT a, b FROM t ORDER BY a, b");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1)],
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(3)],
            vec![Value::Integer(2), Value::Integer(1)],
        ]
    );
}
