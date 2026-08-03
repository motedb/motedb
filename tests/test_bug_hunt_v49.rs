//! Bug Hunt v49 — NULL three-valued logic + CTE in UNION left branch.
//!
//! Two clusters of bugs fixed in this round:
//!
//! 1. **NULL comparison operators returned Bool(false) instead of NULL.**
//!    `SELECT NULL = NULL`, `NULL <> 1`, `NULL < 5` all yielded `Bool(false)`.
//!    Per the SQL standard these are UNKNOWN, represented as NULL. This was
//!    harmless for WHERE filtering (NULL predicate ≡ no match) but wrong for
//!    projection (`SELECT NULL = NULL` → should be NULL, not FALSE). Same fix
//!    applied to `NOT NULL` (was `Bool(true)`, now `NULL`).
//!
//! 2. **CTE was invisible to the LEFT branch of a UNION.** In
//!    `WITH a, b SELECT FROM a UNION SELECT FROM b`, the left arm
//!    (`SELECT FROM a`) ran through `execute_set_op` → `execute_select_internal`
//!    with no CTE rewriting, so table 'a' (and 'b' on the left in chained
//!    unions) was not found.

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
// NULL comparison operators → NULL (UNKNOWN)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_null_eq_null_is_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL = NULL");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_null_ne_concrete_is_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL <> 1, 1 <> NULL");
    assert_eq!(r, vec![vec![Value::Null, Value::Null]]);
}

#[test]
fn test_null_ordering_ops_are_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL < 1, NULL > 1, NULL <= 1, NULL >= 1");
    assert_eq!(
        r,
        vec![vec![Value::Null, Value::Null, Value::Null, Value::Null]]
    );
}

#[test]
fn test_null_eq_concrete_both_sides() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NULL = 5, 5 = NULL");
    assert_eq!(r, vec![vec![Value::Null, Value::Null]]);
}

#[test]
fn test_null_column_comparison_is_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL, 1), (1, NULL), (NULL, NULL), (1, 1)")
        .unwrap();
    // SELECT the comparison result; NULL involved → NULL
    let r = q(&db, "SELECT a = b FROM t ORDER BY a, b");
    // rows ordered by a,b with NULLs first: (NULL,NULL),(NULL,1),(1,NULL),(1,1)
    assert_eq!(
        r,
        vec![
            vec![Value::Null],       // NULL = NULL
            vec![Value::Null],       // NULL = 1
            vec![Value::Null],       // 1 = NULL
            vec![Value::Bool(true)], // 1 = 1
        ]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// NOT NULL → NULL
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_not_null_is_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NOT NULL");
    assert_eq!(r, vec![vec![Value::Null]]);
}

#[test]
fn test_not_concrete_still_works() {
    let (db, _d) = db();
    let r = q(&db, "SELECT NOT 1, NOT 0");
    assert_eq!(r, vec![vec![Value::Bool(false), Value::Bool(true)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// WHERE still treats NULL predicate as no-match (unchanged behavior)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_where_null_comparison_no_match() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    // WHERE a = NULL → matches nothing (NULL predicate = no match)
    let r = q(&db, "SELECT a FROM t WHERE a = NULL");
    assert!(r.is_empty());
    // WHERE a <> 1 should still return 2,3 (no NULLs involved)
    let r2 = q(&db, "SELECT a FROM t WHERE a <> 1 ORDER BY a");
    assert_eq!(r2, vec![vec![Value::Integer(2)], vec![Value::Integer(3)]]);
}

#[test]
fn test_where_with_null_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(a INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (NULL), (2)").unwrap();
    // WHERE a = 1 → only the 1 row (NULL and 2 don't match)
    let r = q(&db, "SELECT a FROM t WHERE a = 1");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
    // WHERE a <> 1 → only 2 (NULL = UNKNOWN → no match)
    let r2 = q(&db, "SELECT a FROM t WHERE a <> 1");
    assert_eq!(r2, vec![vec![Value::Integer(2)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// IS NULL / IS NOT NULL unchanged
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_is_null_predicates() {
    let (db, _d) = db();
    let r = q(
        &db,
        "SELECT NULL IS NULL, 1 IS NULL, NULL IS NOT NULL, 1 IS NOT NULL",
    );
    assert_eq!(
        r,
        vec![vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Bool(false),
            Value::Bool(true),
        ]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// AND / OR three-valued logic (was already correct; regression guard)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_and_or_null_three_valued() {
    let (db, _d) = db();
    let r = q(&db, "SELECT 1 AND NULL, 0 AND NULL, 1 OR NULL, 0 OR NULL");
    assert_eq!(
        r,
        vec![vec![
            Value::Null,        // TRUE AND UNKNOWN = UNKNOWN
            Value::Bool(false), // FALSE AND UNKNOWN = FALSE
            Value::Bool(true),  // TRUE OR UNKNOWN = TRUE
            Value::Null,        // FALSE OR UNKNOWN = UNKNOWN
        ]]
    );
}

// ─────────────────────────────────────────────────────────────────────────
// CTE visible in UNION left branch
// ─────────────────────────────────────────────────────────────────────────

fn sales_db() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE sales (id INT PRIMARY KEY, cat TEXT, qty INT, region TEXT)")
        .unwrap();
    db.execute(
        "INSERT INTO sales VALUES \
         (1,'a',10,'east'), (2,'a',20,'west'), (3,'b',30,'east'), \
         (4,'b',40,'west'), (5,'c',50,'east')",
    )
    .unwrap();
    (db, dir)
}

#[test]
fn test_cte_visible_in_union_left_branch() {
    // Regression: `WITH x SELECT FROM x UNION SELECT FROM x` — left arm
    // previously could not see CTE 'x'.
    let (db, _d) = sales_db();
    let r = q(
        &db,
        "WITH x AS (SELECT id FROM sales WHERE qty > 25) \
         SELECT id FROM x UNION SELECT id FROM x ORDER BY id",
    );
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![3, 4, 5]);
}

#[test]
fn test_cte_visible_in_union_all_left_branch() {
    let (db, _d) = sales_db();
    let r = q(
        &db,
        "WITH x AS (SELECT id FROM sales WHERE qty > 25) \
         SELECT id FROM x UNION ALL SELECT id FROM x ORDER BY id",
    );
    assert_eq!(r.len(), 6); // 3,3,4,4,5,5
}

#[test]
fn test_multiple_ctes_in_union() {
    // Regression: two CTEs a, b — left arm references a, right arm references b.
    let (db, _d) = sales_db();
    let r = q(
        &db,
        "WITH a AS (SELECT id FROM sales WHERE cat = 'a'), \
                  b AS (SELECT id FROM sales WHERE cat = 'b') \
         SELECT id FROM a UNION SELECT id FROM b ORDER BY id",
    );
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test]
fn test_cte_in_intersect_left_branch() {
    let (db, _d) = sales_db();
    let r = q(
        &db,
        "WITH x AS (SELECT id FROM sales WHERE qty >= 20) \
         SELECT id FROM x INTERSECT SELECT id FROM sales WHERE region = 'east' ORDER BY id",
    );
    // x: ids 2,3,4,5 (qty>=20); east: 1,3,5; INTERSECT: 3,5
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![3, 5]);
}

#[test]
fn test_chained_union_with_cte() {
    // Three-way UNION with a CTE visible to all branches.
    let (db, _d) = sales_db();
    let r = q(
        &db,
        "WITH x AS (SELECT id, cat FROM sales) \
         SELECT id FROM x WHERE cat = 'a' \
         UNION SELECT id FROM x WHERE cat = 'b' \
         UNION SELECT id FROM x WHERE cat = 'c' ORDER BY id",
    );
    let ids: Vec<i64> = r
        .iter()
        .map(|row| match row[0] {
            Value::Integer(n) => n,
            _ => panic!(),
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}
