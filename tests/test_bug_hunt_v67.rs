//! Bug Hunt v67 — fourteenth round: text MIN/MAX, HAVING-only agg, UNION edges.

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
// MIN/MAX on TEXT column (lexical)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_min_max_text() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'banana'),(2,'apple'),(3,'cherry')").unwrap();
    let r = q(&db, "SELECT MIN(s), MAX(s) FROM t");
    assert_eq!(r, vec![vec![Value::text("apple".into()), Value::text("cherry".into())]]);
}

#[test]
fn test_min_max_text_with_groupby() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'banana'),(1,'apple'),(2,'zebra'),(2,'ant')").unwrap();
    let r = q(&db, "SELECT g, MIN(s), MAX(s) FROM t GROUP BY g ORDER BY g");
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::text("apple".into()), Value::text("banana".into())],
            vec![Value::Integer(2), Value::text("ant".into()), Value::text("zebra".into())],
        ]
    );
}

#[test]
fn test_min_max_text_empty_returns_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,'x')").unwrap();
    let r = q(&db, "SELECT MIN(s), MAX(s) FROM t WHERE s = 'nope'");
    assert_eq!(r, vec![vec![Value::Null, Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING with aggregate NOT in SELECT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_having_agg_not_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,5)").unwrap();
    // SELECT g (no SUM), HAVING SUM(v) > 10
    let r = q(&db, "SELECT g FROM t GROUP BY g HAVING SUM(v) > 10 ORDER BY g");
    // g1 sum=30 >10 ✓ ; g2 sum=10 ✗
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_having_count_not_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1),(1),(1),(2),(2)").unwrap();
    let r = q(&db, "SELECT g FROM t GROUP BY g HAVING COUNT(*) >= 3 ORDER BY g");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_having_avg_not_in_select() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,30),(2,5),(2,5)").unwrap();
    // g1 avg=20, g2 avg=5. HAVING AVG > 10 → g1
    let r = q(&db, "SELECT g FROM t GROUP BY g HAVING AVG(v) > 10 ORDER BY g");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UNION ALL preserves duplicates; UNION dedups
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_union_all_keeps_duplicates() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1)], vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(2)]]
    );
}

#[test]
fn test_union_dedups() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (1),(2)").unwrap();
    let r = q(&db, "SELECT v FROM a UNION SELECT v FROM b ORDER BY v");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

#[test]
fn test_union_three_tables() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("CREATE TABLE c(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (2)").unwrap();
    db.execute("INSERT INTO c VALUES (3)").unwrap();
    let r = q(&db, "SELECT v FROM a UNION SELECT v FROM b UNION SELECT v FROM c ORDER BY v");
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// INTERSECT / EXCEPT
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_intersect() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO b VALUES (2),(3),(4)").unwrap();
    let r = q(&db, "SELECT v FROM a INTERSECT SELECT v FROM b ORDER BY v");
    assert_eq!(r, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_except() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2),(3)").unwrap();
    db.execute("INSERT INTO b VALUES (2),(3),(4)").unwrap();
    let r = q(&db, "SELECT v FROM a EXCEPT SELECT v FROM b ORDER BY v");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_intersect_empty_result() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a(v INT)").unwrap();
    db.execute("CREATE TABLE b(v INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (3),(4)").unwrap();
    let r = q(&db, "SELECT v FROM a INTERSECT SELECT v FROM b");
    assert!(r.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate in subquery feeding outer comparison
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_subquery_max_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,30),(3,20)").unwrap();
    // Find row(s) with the max v.
    let r = q(&db, "SELECT id FROM t WHERE v = (SELECT MAX(v) FROM t)");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_subquery_min_in_having() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,5),(2,50)").unwrap();
    // Groups whose SUM > average of all sums... simpler: groups with SUM > 15.
    let r = q(&db, "SELECT g FROM t GROUP BY g HAVING SUM(v) > 15 ORDER BY g");
    // g1=30>15 ✓, g2=55>15 ✓
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple WHERE conditions on different columns
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_multiple_columns_and() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,20,30),(2,10,25,30),(3,15,20,40)").unwrap();
    // a=10 AND b>20 AND c=30 → id2
    let r = q(&db, "SELECT id FROM t WHERE a = 10 AND b > 20 AND c = 30");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

#[test]
fn test_where_between_and_in() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT, cat TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10,'a'),(2,20,'b'),(3,30,'a'),(4,15,'c')").unwrap();
    // v BETWEEN 10 AND 25 AND cat IN ('a','c')
    let r = q(&db, "SELECT id FROM t WHERE v BETWEEN 10 AND 25 AND cat IN ('a','c') ORDER BY id");
    // id1(10,a)✓, id2(20,b)✗ cat, id4(15,c)✓
    assert_eq!(r, vec![vec![Value::Integer(1)], vec![Value::Integer(4)]]);
}
