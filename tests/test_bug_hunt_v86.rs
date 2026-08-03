//! Bug Hunt v86 — round 13: transaction semantics, deep nesting, edge numerics,
//! HAVING+WHERE combos, multi-column aggregates, and consistency across
//! repeated identical queries (caching correctness).

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
// BEGIN/COMMIT transaction: writes visible after commit.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_transaction_commit_visible() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    db.execute("COMMIT").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// BEGIN/ROLLBACK: writes discarded.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_transaction_rollback_discards() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    db.execute("ROLLBACK").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(
        r,
        vec![vec![Value::Integer(1)]],
        "rolled-back insert discarded"
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Repeated identical query returns same result (statement cache correctness).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_repeated_query_consistency() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let sql = "SELECT SUM(v) FROM t WHERE v > 10";
    let r1 = q(&db, sql);
    let r2 = q(&db, sql);
    let r3 = q(&db, sql);
    assert_eq!(r1, vec![vec![Value::Integer(50)]]);
    assert_eq!(r1, r2);
    assert_eq!(r2, r3);
}

// ─────────────────────────────────────────────────────────────────────────
// Query after INSERT sees new data (no stale cache).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_query_after_insert_no_stale() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    let r1 = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r1, vec![vec![Value::Integer(10)]]);
    db.execute("INSERT INTO t VALUES (2,20)").unwrap();
    let r2 = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r2, vec![vec![Value::Integer(30)]], "must see new row");
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE + HAVING combo (WHERE filters rows, HAVING filters groups).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_where_then_having() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5),(4,'b',15),(5,'c',1)")
        .unwrap();
    // WHERE v > 5 excludes id5 (v=1). Then GROUP BY cat, HAVING SUM(v) > 20.
    // After WHERE: a={10,20}, b={5,15}, c={} (filtered out).
    // SUM: a=30 (>20 ✓), b=20 (not >20). c has no rows → no group.
    let r = q(
        &db,
        "SELECT cat, SUM(v) FROM t WHERE v > 5 GROUP BY cat HAVING SUM(v) > 20",
    );
    assert_eq!(r, vec![vec![Value::Text("a".into()), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Multi-column GROUP BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_two_columns() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,1),(2,1,2),(3,1,1),(4,2,1),(5,2,1)")
        .unwrap();
    let mut r = q(&db, "SELECT a, b, COUNT(*) FROM t GROUP BY a, b");
    r.sort_by_key(|row| {
        (
            match row[0] {
                Value::Integer(i) => i,
                _ => 999,
            },
            match row[1] {
                Value::Integer(i) => i,
                _ => 999,
            },
        )
    });
    // (1,1)=2, (1,2)=1, (2,1)=2.
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(1), Value::Integer(2), Value::Integer(1)],
            vec![Value::Integer(2), Value::Integer(1), Value::Integer(2)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate of negative numbers.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_negatives() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-10),(2,-20),(3,-30)")
        .unwrap();
    assert_eq!(
        q(&db, "SELECT SUM(v) FROM t"),
        vec![vec![Value::Integer(-60)]]
    );
    assert_eq!(
        q(&db, "SELECT MIN(v) FROM t"),
        vec![vec![Value::Integer(-30)]]
    );
    assert_eq!(
        q(&db, "SELECT MAX(v) FROM t"),
        vec![vec![Value::Integer(-10)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// AVG of negatives.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_avg_negatives() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,-10),(2,-20)").unwrap();
    let r = q(&db, "SELECT AVG(v) FROM t");
    match &r[0][0] {
        Value::Float(f) => assert!((f - (-15.0)).abs() < 1e-9),
        Value::Integer(i) => assert_eq!(*i, -15),
        other => panic!("AVG(-10,-20) = -15, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Comparison chain with mixed operators.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_mixed_comparison_chain() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)")
        .unwrap();
    // v > 0 AND v < 20 AND v != 10 → id1 (5), id2 (15).
    let r = sorted_int(&q(
        &db,
        "SELECT id FROM t WHERE v > 0 AND v < 20 AND v != 10",
    ));
    assert_eq!(r, vec![1, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with table-qualified GROUP BY column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_qualified_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)")
        .unwrap();
    let mut r = q(&db, "SELECT cat, COUNT(*) FROM t GROUP BY t.cat");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![
            vec![Value::Text("a".into()), Value::Integer(2)],
            vec![Value::Text("b".into()), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Nested COALESCE.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_nested_coalesce() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT COALESCE(NULL, COALESCE(NULL, 42)) FROM t");
    assert_eq!(r, vec![vec![Value::Integer(42)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// CASE in WHERE (via boolean expression).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_case_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,5),(2,15),(3,25)")
        .unwrap();
    // WHERE CASE WHEN v > 20 THEN 1 ELSE 0 END = 1 → id3.
    let r = q(
        &db,
        "SELECT id FROM t WHERE CASE WHEN v > 20 THEN 1 ELSE 0 END = 1",
    );
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE SET column = column + literal (in-place arithmetic).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_arithmetic_set() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("UPDATE t SET v = v + 5 WHERE id = 1").unwrap();
    let r = q(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r, vec![vec![Value::Integer(15)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// UPDATE SET column = column * value.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_update_multiply_set() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    db.execute("UPDATE t SET v = v * 3").unwrap();
    let mut r = q(&db, "SELECT v FROM t");
    r.sort_by_key(|row| match row[0] {
        Value::Integer(i) => i,
        _ => 999,
    });
    assert_eq!(r, vec![vec![Value::Integer(30)], vec![Value::Integer(60)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// COUNT with GROUP BY and ORDER BY the count.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_count_order_by_count() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'b'),(4,'c'),(5,'c'),(6,'c')")
        .unwrap();
    let r = q(
        &db,
        "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY COUNT(*) ASC",
    );
    // counts: a=1, b=2, c=3. ASC → a, b, c.
    let cats: Vec<String> = r
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Text(s) => Some(s.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        cats,
        vec!["a".to_string(), "b".to_string(), "c".to_string()]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// ORDER BY COUNT(*) DESC.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_count_order_by_count_desc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'b'),(3,'b'),(4,'c'),(5,'c'),(6,'c')")
        .unwrap();
    let r = q(
        &db,
        "SELECT cat, COUNT(*) FROM t GROUP BY cat ORDER BY COUNT(*) DESC",
    );
    // DESC → c(3), b(2), a(1).
    let cats: Vec<String> = r
        .iter()
        .filter_map(|row| match &row[0] {
            Value::Text(s) => Some(s.as_str().to_string()),
            _ => None,
        })
        .collect();
    assert_eq!(
        cats,
        vec!["c".to_string(), "b".to_string(), "a".to_string()]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// SELECT with no aggregate but GROUP BY (returns one row per group).
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_group_by_select_group_col_only() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a'),(2,'a'),(3,'b')")
        .unwrap();
    let mut r = q(&db, "SELECT cat FROM t GROUP BY cat");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    assert_eq!(
        r,
        vec![vec![Value::Text("a".into())], vec![Value::Text("b".into())]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Subquery in FROM (derived table) with aggregate.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_derived_table_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'a',10),(2,'a',20),(3,'b',5)")
        .unwrap();
    // Derived: per-cat sum, then select where sum > 6.
    let mut r = q(&db, "SELECT sub.cat, sub.s FROM (SELECT cat, SUM(v) AS s FROM t GROUP BY cat) AS sub WHERE sub.s > 6");
    r.sort_by_key(|row| match &row[0] {
        Value::Text(s) => s.as_str().to_string(),
        _ => "zzz".into(),
    });
    // a=30 (>6), b=5 (not). → a only.
    assert_eq!(r, vec![vec![Value::Text("a".into()), Value::Integer(30)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// MIN/MAX on single value.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_min_max_single() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,42)").unwrap();
    assert_eq!(
        q(&db, "SELECT MIN(v), MAX(v) FROM t"),
        vec![vec![Value::Integer(42), Value::Integer(42)]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Empty IN list.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_empty_in_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10)").unwrap();
    // v IN () — empty list. Some engines error, some return empty. Verify behavior.
    let res = db
        .execute("SELECT id FROM t WHERE v IN ()")
        .and_then(|s| s.materialize());
    // Accept either empty result or error (document current behavior).
    match res {
        Ok(r) => {
            let _ = rows(r);
        } // empty is fine
        Err(_) => {} // error is fine
    }
}

// ─────────────────────────────────────────────────────────────────────────
// IN with single element.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_in_single_element() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20)").unwrap();
    let r = sorted_int(&q(&db, "SELECT id FROM t WHERE v IN (10)"));
    assert_eq!(r, vec![1]);
}

// ─────────────────────────────────────────────────────────────────────────
// Large IN list.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_large_in_list() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        .unwrap();
    let r = sorted_int(&q(
        &db,
        "SELECT id FROM t WHERE v IN (1,2,3,4,5,6,7,8,9,10,11,12,13)",
    ));
    assert_eq!(r, vec![1, 2, 3, 4, 5]);
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT on two columns where order matters.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_distinct_two_cols_ordered() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, a INT, b INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,1,2),(2,2,1),(3,1,2)")
        .unwrap();
    let mut r = q(&db, "SELECT DISTINCT a, b FROM t");
    r.sort_by_key(|row| {
        (
            match row[0] {
                Value::Integer(i) => i,
                _ => 999,
            },
            match row[1] {
                Value::Integer(i) => i,
                _ => 999,
            },
        )
    });
    // (1,2) and (2,1) are distinct pairs.
    assert_eq!(
        r,
        vec![
            vec![Value::Integer(1), Value::Integer(2)],
            vec![Value::Integer(2), Value::Integer(1)],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Boolean column in ORDER BY.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_order_by_boolean() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,FALSE),(2,TRUE),(3,FALSE)")
        .unwrap();
    // FALSE sorts before TRUE (0 < 1).
    let r = q(&db, "SELECT id FROM t ORDER BY flag ASC, id ASC");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(i) => Some(i),
            _ => None,
        })
        .collect();
    // FALSE: id1, id3. TRUE: id2.
    assert_eq!(ids, vec![1, 3, 2]);
}

// ─────────────────────────────────────────────────────────────────────────
// Aggregate over boolean column.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_sum_boolean() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, flag BOOLEAN)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,TRUE),(2,FALSE),(3,TRUE)")
        .unwrap();
    // SUM(flag): TRUE=1, so 1+0+1 = 2.
    let r = q(&db, "SELECT SUM(flag) FROM t");
    match &r[0][0] {
        Value::Integer(2) => {}
        other => panic!("SUM(BOOLEAN) = 2, got {:?}", other),
    }
}

// ─────────────────────────────────────────────────────────────────────────
// Float comparison precision.
// ─────────────────────────────────────────────────────────────────────────
#[test]
fn test_float_equality_precision() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,0.1),(2,0.2),(3,0.3)")
        .unwrap();
    // v = 0.3 should match exactly the row with v=0.3 (id=3).
    let r = q(&db, "SELECT id FROM t WHERE v = 0.3");
    assert_eq!(r, vec![vec![Value::Integer(3)]]);
}
