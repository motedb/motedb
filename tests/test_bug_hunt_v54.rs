//! Bug Hunt v54 — four correctness bugs found via code audit + tests.
//!
//! Bugs fixed:
//!
//! 1. **ROUND(integer, decimals) ignored the decimals argument for Integer
//!    input.** `ROUND(15, -1)` returned 15 instead of 20; `ROUND(14, -1)`
//!    returned 14 instead of 10. The Integer arm of `round()` in the evaluator
//!    returned the value unchanged. Now negative decimals round to the nearest
//!    10^|decimals| with half-away-from-zero semantics (matching the Float arm).
//!
//! 2. **Set-operator precedence was wrong: INTERSECT did not bind tighter than
//!    UNION/EXCEPT.** Per SQL standard `A UNION B INTERSECT C` must parse as
//!    `A UNION (B INTERSECT C)`. The parser built a single left-associative
//!    chain, giving `(A UNION B) INTERSECT C`. Fixed with two-level precedence
//!    climbing (INTERSECT level folds before the UNION/EXCEPT level).
//!
//! 3. **Trailing ORDER BY / LIMIT / OFFSET attached to the rightmost SELECT of
//!    a set operation instead of the whole result.** `SELECT a FROM t1 UNION
//!    SELECT a FROM t2 ORDER BY a DESC LIMIT 2` sorted/limited only t2's rows.
//!    Now the parser attaches these clauses to the outermost SetOp node and the
//!    executor applies them to the combined rows. (Required widening
//!    `SetOp.right` from `SelectStmt` to `Statement` so an INTERSECT chain can
//!    be the right operand of a UNION.)
//!
//! 4. **`x IN (...)` did not coerce Bool↔Int/Float like `x = ...` did.**
//!    `b = 1` matched a BOOLEAN column (eval_binary_op coerces Bool→Int), but
//!    `b IN (1)` used raw `Value::eq`/HashSet lookup and never matched. Fixed
//!    by routing IN/InHashset equality through a coerced comparison helper, in
//!    the evaluator, the compiled-where path, and the col-segment scan path.
//!
//! Also fixed: **GROUP_CONCAT inside a compound expression** (e.g.
//! `GROUP_CONCAT(v) || '!'`) raised "Aggregate function GROUP_CONCAT not yet
//! implemented". `resolve_aggregates_in_expr` only recognized COUNT/SUM/AVG/
//! MIN/MAX/STDDEV/VARIANCE; added GROUP_CONCAT so it gets pre-computed like the
//! others.

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
// Bug 1: ROUND(integer, decimals)
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_round_int_negative_decimals_tens() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ROUND(15, -1)");
    assert_eq!(r, vec![vec![Value::Integer(20)]], "ROUND(15,-1)=20");
}

#[test]
fn test_round_int_negative_decimals_round_down() {
    let (db, _d) = db();
    // 14 rounded to tens → 10 (4 < 5 rounds down).
    let r = q(&db, "SELECT ROUND(14, -1)");
    assert_eq!(r, vec![vec![Value::Integer(10)]], "ROUND(14,-1)=10");
}

#[test]
fn test_round_int_negative_decimals_hundreds() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ROUND(1234, -2)");
    assert_eq!(r, vec![vec![Value::Integer(1200)]], "ROUND(1234,-2)=1200");
}

#[test]
fn test_round_int_negative_decimals_negative_value() {
    let (db, _d) = db();
    // Half-away-from-zero: ROUND(-15, -1) = -20.
    let r = q(&db, "SELECT ROUND(-15, -1)");
    assert_eq!(r, vec![vec![Value::Integer(-20)]], "ROUND(-15,-1)=-20");
}

#[test]
fn test_round_int_positive_decimals_unchanged() {
    let (db, _d) = db();
    // Decimals >= 0 on an integer leaves it unchanged.
    let r = q(&db, "SELECT ROUND(15, 2)");
    assert_eq!(r, vec![vec![Value::Integer(15)]]);
}

#[test]
fn test_round_float_still_works() {
    let (db, _d) = db();
    let r = q(&db, "SELECT ROUND(2.567, 2)");
    assert_eq!(r, vec![vec![Value::Float(2.57)]]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 2: set-operator precedence (INTERSECT binds tighter than UNION/EXCEPT)
// ─────────────────────────────────────────────────────────────────────────

fn setop_db() -> (Database, TempDir) {
    let (db, dir) = db();
    db.execute("CREATE TABLE a(x INT)").unwrap();
    db.execute("CREATE TABLE b(x INT)").unwrap();
    db.execute("CREATE TABLE c(x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1),(2)").unwrap();
    db.execute("INSERT INTO b VALUES (2),(3)").unwrap();
    db.execute("INSERT INTO c VALUES (2),(4)").unwrap();
    (db, dir)
}

fn sorted_ints(r: Vec<Vec<Value>>) -> Vec<i64> {
    let mut v: Vec<i64> = r
        .into_iter()
        .filter_map(|row| match row.into_iter().next() {
            Some(Value::Integer(i)) => Some(i),
            _ => None,
        })
        .collect();
    v.sort();
    v
}

#[test]
fn test_union_then_intersect_precedence() {
    // Standard: a UNION (b INTERSECT c) = {1,2} ∪ {2} = {1,2}.
    // Buggy l-to-r: (a UNION b) INTERSECT c = {1,2,3} ∩ {2,4} = {2}.
    let (db, _d) = setop_db();
    let r = q(
        &db,
        "SELECT x FROM a UNION SELECT x FROM b INTERSECT SELECT x FROM c",
    );
    assert_eq!(sorted_ints(r), vec![1, 2]);
}

#[test]
fn test_intersect_then_union_precedence() {
    // a INTERSECT b UNION c = (a ∩ b) ∪ c = {2} ∪ {2,4} = {2,4}.
    let (db, _d) = setop_db();
    let r = q(
        &db,
        "SELECT x FROM a INTERSECT SELECT x FROM b UNION SELECT x FROM c",
    );
    assert_eq!(sorted_ints(r), vec![2, 4]);
}

#[test]
fn test_except_binds_same_as_union() {
    // EXCEPT has the same precedence as UNION, lower than INTERSECT.
    // a EXCEPT b INTERSECT c = a EXCEPT (b ∩ c) = {1,2} \ {2} = {1}.
    let (db, _d) = setop_db();
    let r = q(
        &db,
        "SELECT x FROM a EXCEPT SELECT x FROM b INTERSECT SELECT x FROM c",
    );
    assert_eq!(sorted_ints(r), vec![1]);
}

#[test]
fn test_pure_intersect_chain() {
    // Regression: a plain INTERSECT chain (no UNION) must still parse.
    let (db, _d) = setop_db();
    let r = q(
        &db,
        "SELECT x FROM a INTERSECT SELECT x FROM b INTERSECT SELECT x FROM c",
    );
    assert_eq!(sorted_ints(r), vec![2]);
}

#[test]
fn test_pure_union_chain() {
    let (db, _d) = setop_db();
    let r = q(
        &db,
        "SELECT x FROM a UNION SELECT x FROM b UNION SELECT x FROM c",
    );
    assert_eq!(sorted_ints(r), vec![1, 2, 3, 4]);
}

#[test]
fn test_left_assoc_same_level() {
    // All-UNION is still left-associative (commutative for UNION anyway).
    let (db, _d) = db();
    db.execute("CREATE TABLE a(x INT)").unwrap();
    db.execute("CREATE TABLE b(x INT)").unwrap();
    db.execute("CREATE TABLE c(x INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (1)").unwrap();
    db.execute("INSERT INTO c VALUES (1)").unwrap();
    let r = q(
        &db,
        "SELECT x FROM a EXCEPT SELECT x FROM b EXCEPT SELECT x FROM c",
    );
    // (a EXCEPT b) EXCEPT c = ({} ) EXCEPT c = {}
    assert!(r.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 3: trailing ORDER BY / LIMIT / OFFSET apply to the whole set result
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_union_outer_order_desc_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t1(x INT)").unwrap();
    db.execute("CREATE TABLE t2(x INT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (5),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1),(9)").unwrap();
    // Whole result {1,3,5,9}, ORDER BY x DESC LIMIT 2 → {9,5}.
    let r = q(
        &db,
        "SELECT x FROM t1 UNION SELECT x FROM t2 ORDER BY x DESC LIMIT 2",
    );
    assert_eq!(sorted_ints(r), vec![5, 9]);
}

#[test]
fn test_union_outer_order_asc() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t1(x INT)").unwrap();
    db.execute("CREATE TABLE t2(x INT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (5),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1),(9)").unwrap();
    let r = q(
        &db,
        "SELECT x FROM t1 UNION SELECT x FROM t2 ORDER BY x ASC",
    );
    assert_eq!(sorted_ints(r), vec![1, 3, 5, 9]);
}

#[test]
fn test_union_all_outer_limit() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t1(x INT)").unwrap();
    db.execute("CREATE TABLE t2(x INT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (10),(20)").unwrap();
    db.execute("INSERT INTO t2 VALUES (30),(40)").unwrap();
    // UNION ALL preserves duplicates; ORDER BY x LIMIT 2 → {10,20}.
    let r = q(
        &db,
        "SELECT x FROM t1 UNION ALL SELECT x FROM t2 ORDER BY x LIMIT 2",
    );
    assert_eq!(sorted_ints(r), vec![10, 20]);
}

#[test]
fn test_intersect_outer_order_limit() {
    let (db, _d) = setop_db();
    // a INTERSECT b = {2}. ORDER BY x DESC LIMIT 5 → {2}.
    let r = q(
        &db,
        "SELECT x FROM a INTERSECT SELECT x FROM b ORDER BY x DESC LIMIT 5",
    );
    assert_eq!(sorted_ints(r), vec![2]);
}

#[test]
fn test_union_order_by_ordinal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t1(x INT)").unwrap();
    db.execute("CREATE TABLE t2(x INT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (5),(3)").unwrap();
    db.execute("INSERT INTO t2 VALUES (1),(9)").unwrap();
    // Whole result {1,3,5,9}; ORDER BY 1 DESC LIMIT 3 → {9,5,3}; sorted {3,5,9}.
    let r = q(
        &db,
        "SELECT x FROM t1 UNION SELECT x FROM t2 ORDER BY 1 DESC LIMIT 3",
    );
    assert_eq!(sorted_ints(r), vec![3, 5, 9]);
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 4: IN coerces Bool↔Int like =
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_bool_in_integer_literal() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(b BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (true),(false),(true)")
        .unwrap();
    // `b = 1` matches the two TRUE rows; `b IN (1)` must match the same.
    let eq = q(&db, "SELECT b FROM t WHERE b = 1");
    let in1 = q(&db, "SELECT b FROM t WHERE b IN (1)");
    assert_eq!(eq.len(), 2);
    assert_eq!(in1.len(), 2, "b IN (1) should match b = 1 (both TRUE rows)");
}

#[test]
fn test_bool_in_integer_zero() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(b BOOLEAN)").unwrap();
    db.execute("INSERT INTO t VALUES (true),(false),(true)")
        .unwrap();
    // b IN (0) matches the FALSE row.
    let r = q(&db, "SELECT b FROM t WHERE b IN (0)");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Bool(false)));
}

#[test]
fn test_int_in_with_bool_literal() {
    // Mirror case: an Integer column matched against a Bool literal list.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (0),(1),(2)").unwrap();
    // x IN (TRUE) should match x=1.
    let r = q(&db, "SELECT x FROM t WHERE x IN (TRUE)");
    assert_eq!(r, vec![vec![Value::Integer(1)]]);
}

#[test]
fn test_bool_in_subquery_integer_set() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(b BOOLEAN)").unwrap();
    db.execute("CREATE TABLE s(v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (true),(false),(true)")
        .unwrap();
    db.execute("INSERT INTO s VALUES (1)").unwrap();
    let r = q(&db, "SELECT b FROM t WHERE b IN (SELECT v FROM s)");
    assert_eq!(r.len(), 2, "BOOLEAN col IN (integer subquery) must coerce");
}

// ─────────────────────────────────────────────────────────────────────────
// Bug 5: GROUP_CONCAT inside a compound expression
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn test_group_concat_concatenated() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20),(2,30)")
        .unwrap();
    let r = q(
        &db,
        "SELECT g, GROUP_CONCAT(v) || '!' FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(r.len(), 2);
    for row in &r {
        if let Value::Text(s) = &row[1] {
            assert!(s.ends_with('!'), "expected '!' suffix, got {s:?}");
        } else {
            panic!("expected text, got {:?}", row[1]);
        }
    }
}

#[test]
fn test_group_concat_with_separator_standalone() {
    // Sanity: standalone GROUP_CONCAT(sep) still works (with a PK, matching the
    // v35 pattern).
    let (db, _d) = db();
    db.execute("CREATE TABLE t(id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    let r = q(&db, "SELECT GROUP_CONCAT(v, ',') FROM t");
    assert_eq!(r.len(), 1);
    if let Value::Text(s) = &r[0][0] {
        // order unspecified, but must contain all three values comma-separated
        assert!(s.contains("10") && s.contains("20") && s.contains("30"));
        assert_eq!(s.matches(',').count(), 2);
    } else {
        panic!("expected text, got {:?}", r[0][0]);
    }
}

#[test]
fn test_sum_in_arithmetic_compound() {
    // SUM (already supported) inside arithmetic must keep working.
    let (db, _d) = db();
    db.execute("CREATE TABLE t(g INT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1,10),(1,20)").unwrap();
    let r = q(&db, "SELECT SUM(v) * 2 FROM t");
    assert_eq!(r, vec![vec![Value::Integer(60)]]);
}
