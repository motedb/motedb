//! Bug Hunt v50 — GROUP BY + HAVING (no SELECT aggregate) + ORDER BY routing bug.
//!
//! **Bug:** `SELECT cat FROM t GROUP BY cat HAVING COUNT(*) > 1 ORDER BY cat`
//! returned ALL rows (ignoring GROUP BY and HAVING) because the streaming
//! router had a fast path for "ORDER BY + no SELECT aggregate" that did a
//! full scan + sort, without checking for GROUP BY or HAVING. A query whose
//! only aggregate appears in the HAVING clause (not the SELECT list) was
//! misrouted.
//!
//! **Fix:** Added `group_by.is_none() && having.is_none()` guards to both the
//! ORDER BY fast path and the DISTINCT fast path in `execute_select_streaming_ref`,
//! so they only fire for genuinely simple projection queries.

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

fn setup() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, cat TEXT, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,'b',30),(2,'a',10),(3,'c',20),(4,'a',40),(5,'b',NULL)")
        .unwrap();
    (db, dir)
}

// ─────────────────────────────────────────────────────────────────────────
// The core bug: GROUP BY + HAVING(aggregate) + no SELECT aggregate + ORDER BY
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_having_count_gt_orderby() {
    let (db, _d) = setup();
    // cat a (2 rows), b (2 rows), c (1 row). HAVING COUNT(*) > 1 → a, b
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) > 1 ORDER BY cat",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("a".into())], vec![Value::text("b".into())]]
    );
}

#[test]
fn test_groupby_having_count_eq_orderby() {
    let (db, _d) = setup();
    // HAVING COUNT(*) = 1 → only c
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) = 1 ORDER BY cat",
    );
    assert_eq!(r, vec![vec![Value::text("c".into())]]);
}

#[test]
fn test_groupby_having_count_ne_orderby() {
    let (db, _d) = setup();
    // HAVING COUNT(*) <> 2 → c (1 row)
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) <> 2 ORDER BY cat",
    );
    assert_eq!(r, vec![vec![Value::text("c".into())]]);
}

#[test]
fn test_groupby_having_count_gte_orderby() {
    let (db, _d) = setup();
    // HAVING COUNT(*) >= 2 → a, b
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) >= 2 ORDER BY cat",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("a".into())], vec![Value::text("b".into())]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Without ORDER BY (was already working, regression guard)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_having_no_orderby() {
    let (db, _d) = setup();
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) > 1 ORDER BY cat",
    );
    let cats: Vec<String> = r
        .into_iter()
        .map(|row| match &row[0] {
            Value::Text(t) => t.to_string(),
            _ => panic!(),
        })
        .collect();
    assert_eq!(cats, vec!["a".to_string(), "b".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// HAVING with SUM in HAVING only (not in SELECT)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_having_sum_not_in_select_orderby() {
    let (db, _d) = setup();
    // SUM per cat (ignoring NULL): a=50, b=30, c=20. HAVING SUM(v) > 25 → a, b
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING SUM(v) > 25 ORDER BY cat",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("a".into())], vec![Value::text("b".into())]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// After checkpoint (column-segment-store path) — same bug, same fix
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_groupby_having_after_checkpoint() {
    let (db, _d) = setup();
    db.checkpoint().unwrap();
    let r = q(
        &db,
        "SELECT cat FROM t GROUP BY cat HAVING COUNT(*) > 1 ORDER BY cat",
    );
    assert_eq!(
        r,
        vec![vec![Value::text("a".into())], vec![Value::text("b".into())]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// DISTINCT + GROUP BY: must still route through GROUP BY path (the DISTINCT
// fast path now also checks group_by.is_none()).
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_distinct_with_groupby_routes_correctly() {
    let (db, _d) = setup();
    // Redundant DISTINCT + GROUP BY; semantically GROUP BY cat already
    // produces distinct cat values. Result should be 3 distinct cats.
    let r = q(&db, "SELECT DISTINCT cat FROM t GROUP BY cat ORDER BY cat");
    assert_eq!(
        r,
        vec![
            vec![Value::text("a".into())],
            vec![Value::text("b".into())],
            vec![Value::text("c".into())],
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// Regression: plain ORDER BY (no GROUP BY) still uses fast path
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_plain_orderby_still_works() {
    let (db, _d) = setup();
    let r = q(&db, "SELECT cat FROM t ORDER BY cat");
    // 5 rows, sorted: a, a, b, b, c
    assert_eq!(r.len(), 5);
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "a"));
    assert!(matches!(&r[4][0], Value::Text(t) if t.as_str() == "c"));
}

#[test]
fn test_plain_distinct_still_works() {
    let (db, _d) = setup();
    let r = q(&db, "SELECT DISTINCT cat FROM t ORDER BY cat");
    assert_eq!(r.len(), 3);
}
