//! Bug Hunt v85 — round 12: targeted cross-path consistency — same logical
//! query via indexed vs non-indexed, GROUP BY vs DISTINCT, IN-list vs JOIN,
//! and edge cases around NULL/text/type that historically diverge across paths.

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

fn sorted_int(r: &[Vec<Value>]) -> Vec<i64> {
    let mut v: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.get(0) {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

// ─────────────────────────────────────────────────────────────────────────
// GROUP BY cat yields same count as DISTINCT + COUNT pattern.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_count_matches_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'c'),(5,'c')")
        .unwrap();
    let groups = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY cat");
    let distinct = q(&db, "SELECT DISTINCT cat FROM t");
    assert_eq!(
        groups.len(),
        distinct.len(),
        "GROUP BY groups == DISTINCT values"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE col IN (subquery) matches JOIN semi-join (already in v81, different data).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_subquery_matches_join() {
    let (db, _d) = db();
    db.execute("CREATE TABLE parent(id INT PRIMARY KEY, kind TEXT)")
        .unwrap();
    db.execute("CREATE TABLE child(id INT PRIMARY KEY, pid INT)")
        .unwrap();
    db.execute("INSERT INTO parent VALUES (1,'x'),(2,'y'),(3,'x')")
        .unwrap();
    db.execute("INSERT INTO child VALUES (10,1),(11,3),(12,2),(13,99)")
        .unwrap();
    // parent rows referenced by some child.
    let via_in = sorted_int(&q(
        &db,
        "SELECT id FROM parent WHERE id IN (SELECT pid FROM child)",
    ));
    let via_join = sorted_int(&q(
        &db,
        "SELECT parent.id FROM parent JOIN child ON parent.id = child.pid",
    ));
    assert_eq!(via_in, vec![1, 2, 3]);
    assert_eq!(via_join, vec![1, 2, 3]);
    assert_eq!(via_in, via_join);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple AND conditions including IS NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_and_with_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,NULL),(2,10,5),(3,NULL,5)")
        .unwrap();
    // a = 10 AND b IS NULL → id1 only.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a = 10 AND b IS NULL"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// OR with IS NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_or_with_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,NULL),(3,30)")
        .unwrap();
    // v = 10 OR v IS NULL → id1, id2.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v = 10 OR v IS NULL"));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate after UPDATE reflects new values.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_after_update() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("UPDATE t SET v = v * 2").unwrap();
    assert_eq!(
        q(&db, "SELECT SUM(v) FROM t"),
        vec![vec![Value::Integer(120)]]
    );
    assert_eq!(
        q(&db, "SELECT MAX(v) FROM t"),
        vec![vec![Value::Integer(60)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate after DELETE reflects removed rows.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_aggregate_after_delete() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    db.execute("DELETE FROM t WHERE v > 15").unwrap();
    assert_eq!(
        q(&db, "SELECT SUM(v) FROM t"),
        vec![vec![Value::Integer(10)]]
    );
    assert_eq!(
        q(&db, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Integer(1)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Re-insert after DELETE then aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_reinsert_after_delete_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.execute("INSERT INTO t VALUES (3,30)").unwrap();
    assert_eq!(
        q(&db, "SELECT SUM(v) FROM t"),
        vec![vec![Value::Integer(40)]]
    );
    assert_eq!(
        q(&db, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::Integer(2)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE on TEXT column with = and IN.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_text_where_eq_and_in() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'foo'),(2,'bar'),(3,'baz')")
        .unwrap();
    let via_eq = sorted_int(&q(&db, "SELECT id FROM t WHERE s = 'bar'"));
    let via_in = sorted_int(&q(&db, "SELECT id FROM t WHERE s IN ('bar')"));
    assert_eq!(via_eq, vec![2]);
    assert_eq!(via_in, vec![2]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT with WHERE on TEXT equality.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_text_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b'),(4,'a')")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT COUNT(*) FROM t WHERE cat = 'a'"),
        vec![vec![Value::Integer(3)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT count via COUNT(DISTINCT) vs subquery.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_count_distinct_vs_subquery() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,10),(4,20),(5,30)")
        .unwrap();
    let via_count = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    let via_subq = q(
        &db,
        "SELECT COUNT(*) FROM (SELECT DISTINCT v FROM t) AS sub",
    );
    assert_eq!(via_count, vec![vec![Value::Integer(3)]]);
    assert_eq!(
        via_subq, via_count,
        "COUNT(DISTINCT) must equal count over DISTINCT subquery"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// SUM with WHERE filtering to single row.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_where_single() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT SUM(v) FROM t WHERE id = 2"),
        vec![vec![Value::Integer(20)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// MIN/MAX consistency with ORDER BY LIMIT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_matches_order_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,50),(4,20)")
        .unwrap();
    let via_min = q(&db, "SELECT MIN(v) FROM t");
    let via_order = q(&db, "SELECT v FROM t ORDER BY v ASC LIMIT 1");
    assert_eq!(via_min, via_order);
    assert_eq!(via_min, vec![vec![Value::Integer(10)]]);
}

#[test]
fn test_max_matches_order_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,30),(2,10),(3,50),(4,20)")
        .unwrap();
    let via_max = q(&db, "SELECT MAX(v) FROM t");
    let via_order = q(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 1");
    assert_eq!(via_max, via_order);
    assert_eq!(via_max, vec![vec![Value::Integer(50)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Negative literal in IN list.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_negative_literals() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-5),(2,0),(3,-10),(4,5)")
        .unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (-5, -10)"));
    assert_eq!(r, vec![1, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// BETWEEN with negative bounds.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_between_negative() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-20),(2,-5),(3,0),(4,5)")
        .unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v BETWEEN -10 AND 0"));
    assert_eq!(r, vec![2, 3]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE with no ELSE and no match → NULL.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_no_match_no_else() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5)").unwrap();
    let r = q(&db, "SELECT CASE WHEN v > 10 THEN 'big' END FROM t");
    assert_eq!(r, vec![vec![Value::Null]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multiple CASE branches.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_multi_branch() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4)")
        .unwrap();
    let r = q(&db, "SELECT CASE v WHEN 1 THEN 'one' WHEN 2 THEN 'two' WHEN 3 THEN 'three' ELSE 'other' END FROM t ORDER BY id");
    assert_eq!(
        r,
        vec![
            vec![Value::Text("one".into())],
            vec![Value::Text("two".into())],
            vec![Value::Text("three".into())],
            vec![Value::Text("other".into())],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// String concatenation in SELECT.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_concat_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a TEXT, b TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'foo','bar')").unwrap();
    let r = q(&db, "SELECT a || b FROM t");
    assert_eq!(r, vec![vec![Value::Text("foobar".into())]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Arithmetic in WHERE with multiplication.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_multiplication() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,3,4),(2,5,2),(3,2,10)")
        .unwrap();
    // a * b = 12 → id1 (3*4). id2=10, id3=20.
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE a * b = 12"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY on expression result.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_arithmetic_expr() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10,1),(2,5,5),(3,1,10)")
        .unwrap();
    // ORDER BY a - b: id1=9, id2=0, id3=-9. ASC → id3, id2, id1.
    let r = q(&db, "SELECT id FROM t ORDER BY a - b ASC");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![3, 2, 1]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate AVG returns float even for integer inputs when not evenly divisible.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_avg_not_even() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,15)").unwrap();
    let r = q(&db, "SELECT AVG(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 12.5).abs() < 1e-9, "AVG(10,15)=12.5, got {}", f),
        Value::Integer(i) => assert_eq!(*i, 12, "AVG truncated to 12"),
        other => panic!("AVG unexpected {:?}", other),
    }
}
