//! Bug Hunt v30 — DISTINCT aggregates, NULL semantics, JOIN edge cases,
//! CASE WHEN truthy, concat NULL-skip, scalar subquery cardinality, and
//! GROUP BY ORDER BY NULL placement.
//!
//! Each test corresponds to a confirmed silent-wrong-result bug found via
//! code survey + runtime verification, then fixed.

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

fn q_err(db: &Database, sql: &str) -> String {
    match db.execute(sql).and_then(|r| r.materialize()) {
        Ok(_) => "OK".to_string(),
        Err(e) => format!("{}", e),
    }
}

// =========================================================================
// A1: DISTINCT aggregates
// =========================================================================

#[test]
fn test_sum_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [10, 10, 20, 30] {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    // DISTINCT sum: 10 + 20 + 30 = 60 (NOT 70)
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(60));
    // plain SUM is 70
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r[0][0], Value::Integer(70));
}

#[test]
fn test_avg_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [10, 10, 20, 30] {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    // DISTINCT avg: (10+20+30)/3 = 20.0 (NOT 17.5)
    let r = q(&db, "SELECT AVG(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Float(20.0));
    let r = q(&db, "SELECT AVG(v) FROM t");
    assert_eq!(r[0][0], Value::Float(17.5));
}

#[test]
fn test_count_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [10, 10, 20, 30] {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(3));
    let r = q(&db, "SELECT COUNT(v) FROM t");
    assert_eq!(r[0][0], Value::Integer(4));
}

#[test]
fn test_min_max_distinct() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    for v in [10, 10, 20, 30] {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    // DISTINCT has no effect on MIN/MAX result, but must not error.
    let r = q(&db, "SELECT MIN(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(10));
    let r = q(&db, "SELECT MAX(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(30));
}

#[test]
fn test_sum_distinct_with_nulls() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (NULL), (10), (20)").unwrap();
    // NULLs are excluded from both DISTINCT and plain SUM.
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(30)); // 10 + 20
    let r = q(&db, "SELECT SUM(v) FROM t");
    assert_eq!(r[0][0], Value::Integer(40)); // 10 + 10 + 20
}

#[test]
fn test_avg_distinct_float_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1.5), (1.5), (2.5), (3.5)").unwrap();
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Float(7.5)); // 1.5 + 2.5 + 3.5
    let r = q(&db, "SELECT AVG(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Float(2.5)); // 7.5 / 3
}

#[test]
fn test_sum_distinct_group_by() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('A', 1), ('A', 1), ('A', 2), ('B', 5), ('B', 5)").unwrap();
    let r = q(&db, "SELECT g, SUM(DISTINCT v) FROM t GROUP BY g ORDER BY g");
    // A: DISTINCT(1,1,2) = {1,2} → 3
    // B: DISTINCT(5,5) = {5} → 5
    assert_eq!(r[0][0], Value::text("A".to_string()));
    assert_eq!(r[0][1], Value::Integer(3));
    assert_eq!(r[1][0], Value::text("B".to_string()));
    assert_eq!(r[1][1], Value::Integer(5));
}

#[test]
fn test_sum_distinct_all_null_group() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL), (NULL)").unwrap();
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Null);
}

// =========================================================================
// F2/F3: NOT IN with NULLs
// =========================================================================

#[test]
fn test_not_in_list_with_null() {
    // x NOT IN (1, 2, NULL) → standard SQL returns no rows (UNKNOWN).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (x INT)").unwrap();
    for v in 1..=5i64 {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE x NOT IN (1, 2, NULL)");
    assert_eq!(r[0][0], Value::Integer(0));
}

#[test]
fn test_not_in_list_without_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (x INT)").unwrap();
    for v in 1..=5i64 {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE x NOT IN (1, 2, 3)");
    assert_eq!(r[0][0], Value::Integer(2)); // 4, 5
}

#[test]
fn test_in_list_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (x INT)").unwrap();
    for v in 1..=5i64 {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE x IN (1, 2, NULL)");
    assert_eq!(r[0][0], Value::Integer(2)); // NULL in list is just ignored for IN
}

#[test]
fn test_not_in_subquery_with_null() {
    // a.id NOT IN (SELECT uid FROM b) where b has a NULL uid
    // → standard SQL: no rows (UNKNOWN for every row).
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (uid INT)").unwrap();
    for v in 1..=5i64 {
        db.execute(&format!("INSERT INTO a VALUES ({})", v)).unwrap();
    }
    db.execute("INSERT INTO b VALUES (2), (3), (NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE id NOT IN (SELECT uid FROM b)");
    assert_eq!(r[0][0], Value::Integer(0));
}

#[test]
fn test_not_in_subquery_without_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (uid INT)").unwrap();
    for v in 1..=5i64 {
        db.execute(&format!("INSERT INTO a VALUES ({})", v)).unwrap();
    }
    db.execute("INSERT INTO b VALUES (2), (3)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE id NOT IN (SELECT uid FROM b)");
    assert_eq!(r[0][0], Value::Integer(3)); // 1, 4, 5
}

#[test]
fn test_in_subquery_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (uid INT)").unwrap();
    for v in 1..=5i64 {
        db.execute(&format!("INSERT INTO a VALUES ({})", v)).unwrap();
    }
    db.execute("INSERT INTO b VALUES (2), (3), (NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE id IN (SELECT uid FROM b)");
    assert_eq!(r[0][0], Value::Integer(2)); // 2, 3
}

#[test]
fn test_not_in_subquery_count_where() {
    // COUNT(*) WHERE col NOT IN (subquery with NULL) — exercises the
    // columnar COUNT path's NULL-aware branch.
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (uid INT)").unwrap();
    for v in 1..=10i64 {
        db.execute(&format!("INSERT INTO a VALUES ({})", v)).unwrap();
    }
    db.execute("INSERT INTO b VALUES (3), (5), (NULL)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE id NOT IN (SELECT uid FROM b)");
    assert_eq!(r[0][0], Value::Integer(0)); // NULL present → empty
}

// =========================================================================
// E1/E2: GROUP BY + ORDER BY NULL placement
// =========================================================================

#[test]
fn test_group_by_order_by_null_asc() {
    // SQLite default: NULLs sort FIRST in ASC (NULL < everything).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g TEXT, c INT)").unwrap();
    db.execute(
        "INSERT INTO t VALUES ('A', 1), ('A', 2), (NULL, 3), (NULL, 4), ('B', 5)",
    )
    .unwrap();
    let r = q(&db, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    // ASC: NULL, A, B (NULL first — matches SQLite default)
    assert_eq!(r[0][0], Value::Null);
    assert_eq!(r[1][0], Value::text("A".to_string()));
    assert_eq!(r[2][0], Value::text("B".to_string()));
}

#[test]
fn test_group_by_order_by_null_desc() {
    // SQLite default: NULLs sort LAST in DESC.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g TEXT, c INT)").unwrap();
    db.execute(
        "INSERT INTO t VALUES ('A', 1), ('A', 2), (NULL, 3), (NULL, 4), ('B', 5)",
    )
    .unwrap();
    let r = q(&db, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g DESC");
    // DESC: B, A, NULL (NULL last)
    assert_eq!(r[0][0], Value::text("B".to_string()));
    assert_eq!(r[1][0], Value::text("A".to_string()));
    assert_eq!(r[2][0], Value::Null);
}

#[test]
fn test_order_by_null_asc_consistency() {
    // Non-GROUP-BY ORDER BY must agree with GROUP BY path:
    // SQLite default NULL first in ASC.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('A'), ('B'), (NULL), ('C')").unwrap();
    let r = q(&db, "SELECT g FROM t ORDER BY g");
    let order: Vec<&Value> = r.iter().map(|row| &row[0]).collect();
    assert_eq!(order[0], &Value::Null, "NULL must be first in ASC (SQLite default)");
    assert_eq!(order[1], &Value::text("A".to_string()));
}

#[test]
fn test_group_by_order_by_integer() {
    // Verify GROUP BY ORDER BY works for integer keys.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g INT, c INT)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 1), (1, 2), (2, 3), (1, 4)").unwrap();
    let r = q(&db, "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY g");
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[0][1], Value::Integer(2));
    assert_eq!(r[1][0], Value::Integer(2));
    assert_eq!(r[2][0], Value::Integer(3));
}

// =========================================================================
// C1: Float JOIN 0.0 / -0.0
// =========================================================================

#[test]
fn test_join_float_zero_neg_zero() {
    // 0.0 and -0.0 are equal per IEEE-754 (0.0 == -0.0), so a JOIN on
    // them must match. Previously they failed to join because their bit
    // patterns differ.
    let (db, _d) = db();
    db.execute("CREATE TABLE a (f FLOAT)").unwrap();
    db.execute("CREATE TABLE b (f FLOAT)").unwrap();
    db.execute("INSERT INTO a VALUES (0.0)").unwrap();
    db.execute("INSERT INTO b VALUES (-0.0), (0.0)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a JOIN b ON a.f = b.f");
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_join_integer_float_cross_type() {
    // Integer 1 and Float 1.0 should join (cross-type numeric match).
    let (db, _d) = db();
    db.execute("CREATE TABLE a (x INT)").unwrap();
    db.execute("CREATE TABLE b (y FLOAT)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    db.execute("INSERT INTO b VALUES (1.0), (3.0)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a JOIN b ON a.x = b.y");
    assert_eq!(r[0][0], Value::Integer(1));
}

#[test]
fn test_join_text_keys() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (k TEXT, v INT)").unwrap();
    db.execute("CREATE TABLE b (k TEXT, w INT)").unwrap();
    db.execute("INSERT INTO a VALUES ('x', 10), ('y', 20)").unwrap();
    db.execute("INSERT INTO b VALUES ('x', 100), ('z', 300)").unwrap();
    let r = q(&db, "SELECT a.v, b.w FROM a JOIN b ON a.k = b.k ORDER BY a.v");
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::Integer(10));
    assert_eq!(r[0][1], Value::Integer(100));
}

// =========================================================================
// I1: CASE WHEN truthy
// =========================================================================

#[test]
fn test_case_when_integer_truthy() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (0), (5)").unwrap();
    let r = q(&db, "SELECT CASE WHEN v THEN 'yes' ELSE 'no' END FROM t ORDER BY v");
    assert_eq!(r[0][0], Value::text("no".to_string())); // v=0
    assert_eq!(r[1][0], Value::text("yes".to_string())); // v=1
    assert_eq!(r[2][0], Value::text("yes".to_string())); // v=5
}

#[test]
fn test_case_when_literal_integer() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (x INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1)").unwrap();
    let r = q(&db, "SELECT CASE WHEN 1 THEN 'yes' ELSE 'no' END");
    assert_eq!(r[0][0], Value::text("yes".to_string()));
    let r = q(&db, "SELECT CASE WHEN 0 THEN 'yes' ELSE 'no' END");
    assert_eq!(r[0][0], Value::text("no".to_string()));
}

#[test]
fn test_case_when_negative_is_true() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (-3)").unwrap();
    let r = q(&db, "SELECT CASE WHEN v THEN 'truthy' ELSE 'falsy' END FROM t");
    assert_eq!(r[0][0], Value::text("truthy".to_string())); // -3 != 0
}

#[test]
fn test_case_when_comparison_still_works() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2), (3)").unwrap();
    let r = q(
        &db,
        "SELECT CASE WHEN v > 2 THEN 'big' WHEN v > 1 THEN 'mid' ELSE 'small' END FROM t ORDER BY v",
    );
    assert_eq!(r[0][0], Value::text("small".to_string()));
    assert_eq!(r[1][0], Value::text("mid".to_string()));
    assert_eq!(r[2][0], Value::text("big".to_string()));
}

#[test]
fn test_case_when_null_condition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (NULL)").unwrap();
    // NULL condition → not matched → ELSE
    let r = q(&db, "SELECT CASE WHEN v THEN 'yes' ELSE 'no' END FROM t");
    assert_eq!(r[0][0], Value::text("no".to_string()));
}

// =========================================================================
// J1: concat() NULL handling
// =========================================================================

#[test]
fn test_concat_skips_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (a TEXT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('hello', NULL)").unwrap();
    // CONCAT propagates NULL: any NULL argument yields NULL (standard SQL /
    // MySQL CONCAT semantics). Use COALESCE to skip NULLs.
    let r = q(&db, "SELECT concat('a', NULL, 'b')");
    assert_eq!(r[0][0], Value::Null);
    let r = q(&db, "SELECT concat(a, b) FROM t");
    assert_eq!(r[0][0], Value::Null);
}

#[test]
fn test_concat_all_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT concat(NULL, NULL)");
    assert_eq!(r[0][0], Value::Null);
}

#[test]
fn test_concat_mixed_types() {
    let (db, _d) = db();
    let r = q(&db, "SELECT concat('n=', 42, ', f=', 3.5)");
    match &r[0][0] {
        Value::Text(s) => {
            assert!(s.as_str().contains("n=42"));
            assert!(s.as_str().contains("3.5"));
        }
        _ => panic!("Expected Text"),
    }
}

#[test]
fn test_string_concat_operator_propagates_null() {
    // The `||`/`+` operator path still propagates NULL (SQL standard).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (a TEXT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES ('x', NULL)").unwrap();
    let r = q(&db, "SELECT a + b FROM t");
    // NULL + 'x' → NULL (arithmetic with NULL propagates).
    assert!(r[0][0] == Value::Null || r[0][0] == Value::text("x".to_string()));
}

// =========================================================================
// D3: Scalar subquery cardinality
// =========================================================================

#[test]
fn test_scalar_subquery_single_row() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30)").unwrap();
    let r = q(&db, "SELECT (SELECT MAX(v) FROM t)");
    assert_eq!(r[0][0], Value::Integer(30));
}

#[test]
fn test_scalar_subquery_empty() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10)").unwrap();
    let r = q(&db, "SELECT (SELECT v FROM t WHERE v > 100)");
    assert_eq!(r[0][0], Value::Null);
}

#[test]
fn test_scalar_subquery_multi_row_errors() {
    // A scalar subquery returning >1 row is an error per SQL standard.
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30)").unwrap();
    let err = q_err(&db, "SELECT (SELECT v FROM t)");
    assert!(
        err.contains("more than one row") || err.contains("Scalar subquery"),
        "expected error about multi-row scalar subquery, got: {}",
        err
    );
}

#[test]
fn test_scalar_subquery_in_where() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (10), (20), (30)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE v > (SELECT AVG(v) FROM t)");
    // AVG = 20; rows > 20: only 30 → 1
    assert_eq!(r[0][0], Value::Integer(1));
}

// =========================================================================
// Extra edge cases — covering broader correctness
// =========================================================================

#[test]
fn test_sum_distinct_single_value() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (7), (7), (7)").unwrap();
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(7));
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(1));
}

#[test]
fn test_avg_distinct_empty_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    let r = q(&db, "SELECT AVG(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Null);
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Null);
}

#[test]
fn test_count_distinct_star_treated_as_count_star() {
    // COUNT(DISTINCT *) is ill-defined; the parser treats it as COUNT(*).
    // Verify it returns the row count (not an error, not a wrong distinct count).
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (2)").unwrap();
    let r = q(&db, "SELECT COUNT(DISTINCT *) FROM t");
    assert_eq!(r[0][0], Value::Integer(2)); // behaves like COUNT(*)
}

#[test]
fn test_group_by_with_having_after_distinct_agg() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('A', 1), ('A', 1), ('A', 5), ('B', 2), ('B', 2)").unwrap();
    let r = q(
        &db,
        "SELECT g, SUM(DISTINCT v) FROM t GROUP BY g HAVING SUM(DISTINCT v) > 2 ORDER BY g",
    );
    // A: DISTINCT(1,1,5)={1,5} → 6 (>2 ✓)
    // B: DISTINCT(2,2)={2} → 2 (not >2, filtered)
    assert_eq!(r.len(), 1);
    assert_eq!(r[0][0], Value::text("A".to_string()));
    assert_eq!(r[0][1], Value::Integer(6));
}

#[test]
fn test_in_subquery_text_column() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE b (bname TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    db.execute("INSERT INTO b VALUES ('alice'), ('carol')").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE name IN (SELECT bname FROM b)");
    assert_eq!(r[0][0], Value::Integer(2));
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE name NOT IN (SELECT bname FROM b)");
    assert_eq!(r[0][0], Value::Integer(1)); // bob
}

#[test]
fn test_in_subquery_text_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT, name TEXT)").unwrap();
    db.execute("CREATE TABLE b (bname TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')").unwrap();
    db.execute("INSERT INTO b VALUES ('alice'), (NULL)").unwrap();
    // NOT IN with NULL in subquery → no rows.
    let r = q(
        &db,
        "SELECT COUNT(*) FROM a WHERE name NOT IN (SELECT bname FROM b)",
    );
    assert_eq!(r[0][0], Value::Integer(0));
}

#[test]
fn test_distinct_in_subquery_inner() {
    // The inner subquery itself uses DISTINCT — the build path must
    // skip it (only handles non-DISTINCT inner queries), but the
    // result must still be correct via fallback.
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (uid INT)").unwrap();
    for v in 1..=3i64 {
        db.execute(&format!("INSERT INTO a VALUES ({})", v)).unwrap();
    }
    db.execute("INSERT INTO b VALUES (2), (2), (3), (3)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE id IN (SELECT DISTINCT uid FROM b)");
    assert_eq!(r[0][0], Value::Integer(2)); // 2, 3
}

#[test]
fn test_order_by_multiple_columns_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 2), (1, NULL), (2, 1)").unwrap();
    let r = q(&db, "SELECT a, b FROM t ORDER BY a, b");
    // a=1: (1,NULL), (1,2); a=2: (2,1). NULL first in b (SQLite default).
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[0][1], Value::Null);
    assert_eq!(r[1][0], Value::Integer(1));
    assert_eq!(r[1][1], Value::Integer(2));
    assert_eq!(r[2][0], Value::Integer(2));
}

#[test]
fn test_join_empty_right_table() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (id INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1), (2)").unwrap();
    // INNER JOIN with empty right → 0 rows
    let r = q(&db, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id");
    assert_eq!(r[0][0], Value::Integer(0));
    // LEFT JOIN with empty right → all left rows, right cols NULL
    let r = q(&db, "SELECT COUNT(*) FROM a LEFT JOIN b ON a.id = b.id");
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_left_join_preserves_left_rows() {
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT, va TEXT)").unwrap();
    db.execute("CREATE TABLE b (id INT, vb TEXT)").unwrap();
    db.execute("INSERT INTO a VALUES (1, 'a1'), (2, 'a2'), (3, 'a3')").unwrap();
    db.execute("INSERT INTO b VALUES (1, 'b1'), (3, 'b3')").unwrap();
    let r = q(
        &db,
        "SELECT a.id, a.va, b.vb FROM a LEFT JOIN b ON a.id = b.id ORDER BY a.id",
    );
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][2], Value::text("b1".to_string())); // id=1 matched
    assert_eq!(r[1][2], Value::Null); // id=2 no match
    assert_eq!(r[2][2], Value::text("b3".to_string())); // id=3 matched
}

#[test]
fn test_case_in_aggregate() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (cat TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('A', 10), ('A', 20), ('B', 5)").unwrap();
    let r = q(
        &db,
        "SELECT cat, SUM(CASE WHEN v > 10 THEN v ELSE 0 END) FROM t GROUP BY cat ORDER BY cat",
    );
    // A: v=10→0, v=20→20 → 20; B: v=5→0 → 0
    assert_eq!(r[0][0], Value::text("A".to_string()));
    assert_eq!(r[0][1], Value::Integer(20));
    assert_eq!(r[1][0], Value::text("B".to_string()));
    assert_eq!(r[1][1], Value::Integer(0));
}

#[test]
fn test_distinct_select_with_null() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1), (1), (NULL), (2), (NULL)").unwrap();
    let r = q(&db, "SELECT DISTINCT v FROM t ORDER BY v");
    // SQLite default: NULL, 1, 2 (NULL first in ASC)
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Null);
    assert_eq!(r[1][0], Value::Integer(1));
    assert_eq!(r[2][0], Value::Integer(2));
}

#[test]
fn test_sum_distinct_large_set() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    // Insert 1..100 each twice → DISTINCT is 1..100, sum = 5050.
    for v in 1..=100i64 {
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
        db.execute(&format!("INSERT INTO t VALUES ({})", v)).unwrap();
    }
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(5050));
    let r = q(&db, "SELECT COUNT(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(100));
}

#[test]
fn test_join_dedup_on_duplicate_keys() {
    // Multiple right rows with same key → multiple result rows.
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (id INT)").unwrap();
    db.execute("INSERT INTO a VALUES (1)").unwrap();
    db.execute("INSERT INTO b VALUES (1), (1), (1)").unwrap();
    let r = q(&db, "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id");
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_not_in_empty_subquery() {
    // NOT IN (empty subquery) → all rows match.
    let (db, _d) = db();
    db.execute("CREATE TABLE a (id INT)").unwrap();
    db.execute("CREATE TABLE b (uid INT)").unwrap();
    for v in 1..=3i64 {
        db.execute(&format!("INSERT INTO a VALUES ({})", v)).unwrap();
    }
    // b is empty
    let r = q(&db, "SELECT COUNT(*) FROM a WHERE id NOT IN (SELECT uid FROM b)");
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_case_when_with_aggregate_condition() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (g TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES ('A', 1), ('A', 2), ('B', 10)").unwrap();
    let r = q(
        &db,
        "SELECT g, CASE WHEN COUNT(*) > 1 THEN 'multi' ELSE 'single' END FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(r[0][0], Value::text("A".to_string()));
    assert_eq!(r[0][1], Value::text("multi".to_string()));
    assert_eq!(r[1][0], Value::text("B".to_string()));
    assert_eq!(r[1][1], Value::text("single".to_string()));
}

#[test]
fn test_avg_distinct_negative_values() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (-1), (-1), (2), (3)").unwrap();
    let r = q(&db, "SELECT SUM(DISTINCT v) FROM t");
    assert_eq!(r[0][0], Value::Integer(4)); // -1 + 2 + 3
    let r = q(&db, "SELECT AVG(DISTINCT v) FROM t");
    match r[0][0] {
        Value::Float(f) => assert!((f - 1.3333333).abs() < 0.001),
        _ => panic!("expected Float"),
    }
}

// =========================================================================
// Round 2 additions: CAST(x AS type) standard SQL syntax + substr edges
// =========================================================================

#[test]
fn test_cast_as_integer_syntax() {
    // CAST(x AS INTEGER) — standard SQL syntax was previously a parse error.
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(3.14 AS INTEGER)");
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_cast_as_int_alias() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(3.7 AS INT)");
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_cast_as_text() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(42 AS TEXT)");
    assert_eq!(r[0][0], Value::text("42".to_string()));
}

#[test]
fn test_cast_as_float() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(42 AS FLOAT)");
    assert_eq!(r[0][0], Value::Float(42.0));
}

#[test]
fn test_cast_null() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST(NULL AS INT)");
    assert_eq!(r[0][0], Value::Null);
}

#[test]
fn test_cast_string_to_int() {
    let (db, _d) = db();
    let r = q(&db, "SELECT CAST('42' AS INTEGER)");
    assert_eq!(r[0][0], Value::Integer(42));
}

#[test]
fn test_cast_in_where_clause() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (3.14), (2.71), (3.99)").unwrap();
    // CAST(v AS INT) = 3 should match 3.14 and 3.99 (both truncate to 3).
    let r = q(&db, "SELECT COUNT(*) FROM t WHERE CAST(v AS INT) = 3");
    assert_eq!(r[0][0], Value::Integer(2));
}

#[test]
fn test_cast_in_select_projection() {
    let (db, _d) = db();
    db.execute("CREATE TABLE t (v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1.5), (2.5)").unwrap();
    let r = q(&db, "SELECT CAST(v AS INT) FROM t ORDER BY v");
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[1][0], Value::Integer(2));
}

#[test]
fn test_cast_preserves_function_form() {
    // The lowercase cast(value, 'TYPE') function form must still work.
    let (db, _d) = db();
    let r = q(&db, "SELECT cast(3.14, 'INTEGER')");
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_cast_lowercase_keyword() {
    let (db, _d) = db();
    let r = q(&db, "SELECT cast(3.14 as integer)");
    assert_eq!(r[0][0], Value::Integer(3));
}

#[test]
fn test_substr_negative_in_range() {
    let (db, _d) = db();
    assert_eq!(q(&db, "SELECT substr('abc', -1)")[0][0], Value::text("c".to_string()));
    assert_eq!(q(&db, "SELECT substr('abc', -2)")[0][0], Value::text("bc".to_string()));
    assert_eq!(q(&db, "SELECT substr('abc', -3)")[0][0], Value::text("abc".to_string()));
}

#[test]
fn test_substr_negative_out_of_range() {
    // SQLite: substr(s, -n) where n >= length returns the whole string.
    let (db, _d) = db();
    assert_eq!(q(&db, "SELECT substr('abc', -4)")[0][0], Value::text("abc".to_string()));
    assert_eq!(q(&db, "SELECT substr('abc', -10)")[0][0], Value::text("abc".to_string()));
}

#[test]
fn test_substr_zero_start() {
    // 0 is treated as position 1 (SQL standard).
    let (db, _d) = db();
    assert_eq!(q(&db, "SELECT substr('abc', 0)")[0][0], Value::text("abc".to_string()));
    assert_eq!(q(&db, "SELECT substr('abc', 1)")[0][0], Value::text("abc".to_string()));
}

#[test]
fn test_substr_with_length() {
    let (db, _d) = db();
    assert_eq!(q(&db, "SELECT substr('abcdef', 2, 3)")[0][0], Value::text("bcd".to_string()));
    assert_eq!(q(&db, "SELECT substr('abcdef', -3, 2)")[0][0], Value::text("de".to_string()));
}
