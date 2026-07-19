//! Bug-hunt v18: CTE edge cases, correlated/IN subquery NULL semantics,
//! CASE expression NULL propagation, mixed-type arithmetic, and self-join.
//!
//! Goal: surface silent wrong-result bugs. Each test asserts the SQL-standard
//! expected behavior; a panic means either a real bug OR (sometimes) a test
//! that needs adjustment. Investigate each failure individually.

use motedb::sql::QueryResult;
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) {
    db.execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e));
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap_or_else(|e| panic!("materialize failed: {}\n  err: {}", sql, e));
    match rs {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected Select for: {}", sql),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() {
        Some(Value::Integer(n)) => *n,
        Some(Value::Null) => panic!("got NULL, expected int: {}", sql),
        o => panic!("expected int, got {:?}: {}", o, sql),
    }
}

fn scalar_f64(db: &Database, sql: &str) -> f64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    match r[0].first() {
        Some(Value::Float(n)) => *n,
        Some(Value::Integer(n)) => *n as f64,
        Some(Value::Null) => panic!("got NULL, expected float: {}", sql),
        o => panic!("expected float, got {:?}: {}", o, sql),
    }
}

fn scalar_is_null(db: &Database, sql: &str) -> bool {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row: {}", sql);
    matches!(r[0].first(), Some(Value::Null))
}

/// Test data setup: emp table with NULL salary, dept table.
fn emp_db() -> (Database, TempDir) {
    let (db, dir) = new_db();
    exec(
        &db,
        "CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, dept_id INT, salary INT)",
    );
    exec(&db, "INSERT INTO emp VALUES \
        (1, 'alice', 10, 100), \
        (2, 'bob',   10, 200), \
        (3, 'carol', 20, 150), \
        (4, 'dave',  20, NULL), \
        (5, 'eve',   NULL, 300)");
    exec(
        &db,
        "CREATE TABLE dept (id INT PRIMARY KEY, name TEXT, budget INT)",
    );
    exec(&db, "INSERT INTO dept VALUES \
        (10, 'eng', 1000), \
        (20, 'sales', 500), \
        (30, 'hr', 200)");
    (db, dir)
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: CTE edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_empty_result() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id FROM emp WHERE salary > 1000) SELECT * FROM x",
    );
    assert_eq!(r.len(), 0);
}

#[test]
fn cte_count_zero() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "WITH x AS (SELECT id FROM emp WHERE salary > 9999) SELECT COUNT(*) FROM x");
    assert_eq!(n, 0);
}

#[test]
fn cte_with_null_in_projection() {
    let (db, _dir) = emp_db();
    // dave has NULL salary; CTE projects id, salary; main query counts NULLs.
    let n = scalar_i64(
        &db,
        "WITH x AS (SELECT id, salary FROM emp) \
         SELECT COUNT(*) FROM x WHERE salary IS NULL",
    );
    assert_eq!(n, 1, "dave has NULL salary");
}

#[test]
fn cte_aggregate_then_filter() {
    let (db, _dir) = emp_db();
    // Per-dept max salary, then filter depts where max >= 200.
    let r = rows(
        &db,
        "WITH dm AS (SELECT dept_id, MAX(salary) AS m FROM emp WHERE dept_id IS NOT NULL GROUP BY dept_id) \
         SELECT dept_id, m FROM dm WHERE m >= 200 ORDER BY dept_id",
    );
    // dept 10: max(100,200)=200; dept 20: max(150, NULL)=150. So only dept 10.
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Integer(10)));
    assert!(matches!(&r[0][1], Value::Integer(200)));
}

#[test]
fn cte_join_with_aggregate_in_main() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(
        &db,
        "WITH e AS (SELECT id, dept_id FROM emp) \
         SELECT COUNT(*) FROM e JOIN dept ON e.dept_id = dept.id",
    );
    // emp rows where dept_id is not NULL and matches a dept: 1,2,3,4 → 4 rows.
    assert_eq!(n, 4);
}

#[test]
fn cte_referenced_in_subquery_where() {
    let (db, _dir) = emp_db();
    // v1 limitation: nested subquery does NOT see CTE — this should error
    // rather than silently return wrong results.
    let result = db.execute(
        "WITH high AS (SELECT id FROM emp WHERE salary > 150) \
         SELECT id FROM emp WHERE id IN (SELECT id FROM high)",
    );
    // Either works (CTE visible to nested subquery) or errors clearly.
    // v1: should error because nested subquery can't see CTE.
    match result {
        Ok(_) => {
            // If it works, must return correct result (ids 2, 3, 5 have salary > 150).
            let r = result.unwrap().materialize().unwrap();
            if let QueryResult::Select { rows, .. } = r {
                let ids: Vec<i64> = rows
                    .iter()
                    .filter_map(|r| match r.first() {
                        Some(Value::Integer(n)) => Some(*n),
                        _ => None,
                    })
                    .collect();
                let mut ids_sorted = ids.clone();
                ids_sorted.sort();
                assert_eq!(ids_sorted, vec![2, 3, 5]);
            }
        }
        Err(_) => {
            // Acceptable: v1 limitation.
        }
    }
}

#[test]
fn cte_used_in_update_rejected() {
    let (db, _dir) = emp_db();
    let result = db.execute(
        "WITH x AS (SELECT 1) UPDATE emp SET salary = 0 WHERE id = 1",
    );
    assert!(result.is_err(), "WITH before UPDATE should be rejected");
}

#[test]
fn cte_with_limit_inside_body() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id FROM emp ORDER BY id LIMIT 3) \
         SELECT id FROM x ORDER BY id",
    );
    assert_eq!(r.len(), 3);
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[2][0], Value::Integer(3)));
}

#[test]
fn cte_distinct() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(
        &db,
        "WITH x AS (SELECT dept_id FROM emp) \
         SELECT COUNT(DISTINCT dept_id) FROM x",
    );
    // dept_ids: 10, 10, 20, 20, NULL. DISTINCT ignores NULL → 2 distinct values.
    // But COUNT(DISTINCT col) standard SQL: ignores NULL. So 2.
    assert_eq!(n, 2);
}

#[test]
fn cte_with_case_in_body() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id, CASE WHEN salary IS NULL THEN 'unknown' ELSE 'known' END AS s FROM emp) \
         SELECT id FROM x WHERE s = 'unknown'",
    );
    assert_eq!(r.len(), 1, "only dave has NULL salary");
    assert!(matches!(&r[0][0], Value::Integer(4)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: NULL semantics in WHERE / aggregate / arithmetic
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn null_arithmetic_propagates_null() {
    let (db, _dir) = emp_db();
    // dave.salary IS NULL → NULL + 1 should be NULL, not 1.
    assert!(scalar_is_null(
        &db,
        "SELECT salary + 1 FROM emp WHERE id = 4"
    ));
}

#[test]
fn null_comparison_is_unknown() {
    let (db, _dir) = emp_db();
    // NULL = NULL is unknown, not true. So WHERE salary = NULL matches nothing.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM emp WHERE salary = NULL");
    assert_eq!(n, 0, "salary = NULL must match no rows (use IS NULL)");
}

#[test]
fn null_in_in_list() {
    let (db, _dir) = emp_db();
    // SQL standard: x IN (a, NULL) where x not in {a} → NULL (unknown).
    // For COUNT, unknown is not counted. So 0.
    let n = scalar_i64(
        &db,
        "SELECT COUNT(*) FROM emp WHERE salary IN (999, NULL)",
    );
    assert_eq!(n, 0);
}

#[test]
fn aggregate_ignores_null_count() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "SELECT COUNT(salary) FROM emp");
    // 4 non-NULL salaries.
    assert_eq!(n, 4);
}

#[test]
fn aggregate_sum_ignores_null() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "SELECT SUM(salary) FROM emp");
    // 100+200+150+300 = 750 (dave's NULL skipped).
    assert_eq!(n, 750);
}

#[test]
fn aggregate_avg_ignores_null() {
    let (db, _dir) = emp_db();
    let f = scalar_f64(&db, "SELECT AVG(salary) FROM emp");
    // (100+200+150+300)/4 = 187.5
    assert!((f - 187.5).abs() < 0.001);
}

#[test]
fn aggregate_min_ignores_null() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "SELECT MIN(salary) FROM emp");
    assert_eq!(n, 100);
}

#[test]
fn aggregate_max_ignores_null() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "SELECT MAX(salary) FROM emp");
    assert_eq!(n, 300);
}

#[test]
fn groupby_with_null_key() {
    let (db, _dir) = emp_db();
    // GROUP BY dept_id: NULL forms its own group. So 3 groups: 10, 20, NULL.
    let r = rows(
        &db,
        "SELECT dept_id, COUNT(*) FROM emp GROUP BY dept_id ORDER BY dept_id",
    );
    assert_eq!(r.len(), 3, "NULL forms its own group");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: Mixed-type arithmetic and comparison
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn int_plus_float_yields_float() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, i INT, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 10, 0.5)");
    let f = scalar_f64(&db, "SELECT i + f FROM t WHERE id = 1");
    assert!((f - 10.5).abs() < 0.001);
}

#[test]
fn int_div_int_truncates_or_floats() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t VALUES (1, 7, 2)");
    // Many SQL dialects (Postgres) truncate; SQLite returns float. Either is OK
    // but should be consistent. We assert it's either 3 (truncate) or 3.5.
    let r = rows(&db, "SELECT a / b FROM t WHERE id = 1");
    match r[0][0] {
        Value::Integer(3) => { /* postgres-style integer division */ }
        Value::Float(f) if (f - 3.5).abs() < 0.001 => { /* sqlite-style */ }
        ref o => panic!("expected 3 or 3.5, got {:?}", o),
    }
}

#[test]
fn string_comparison_lexicographic() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM emp WHERE name < 'c'");
    // 'alice' < 'c', 'bob' < 'c', 'carol' = 'c' (not <). So 2.
    assert_eq!(n, 2);
}

#[test]
fn text_concatenation_not_supported() {
    // SQL standard `||` for string concatenation is not implemented in v0.5.x.
    // Document the limitation.
    let (db, _dir) = emp_db();
    let result = db.execute("SELECT name || '!' FROM emp WHERE id = 1");
    assert!(result.is_err(), "|| string concatenation is not supported");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: Correlated subquery
//
// ⚠️ KNOWN LIMITATION: MoteDB v0.5.x does not implement correlated subqueries.
// A subquery that references an outer-query column (e.g.
// `SELECT ... WHERE col = (SELECT ... FROM t2 WHERE t2.x = outer.col)`) is
// executed as if it were uncorrelated — the outer reference is silently
// unresolved, producing a single result used for every outer row. This is
// wrong, but documented here so the behavior is at least known. These tests
// are written to verify the *current* behavior so a future fix breaks them.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn correlated_subquery_in_select_currently_uncorrelated() {
    let (db, _dir) = emp_db();
    // CORRECT SQL behavior: each emp should see the max of their own dept.
    // dept 10 max=200 (alice,bob), dept 20 max=150 (carol; dave NULL skipped),
    // NULL dept max=300 (eve alone).
    //
    // CURRENT (BUGGY) BEHAVIOR: the subquery's outer reference
    // (`emp.dept_id`) is unresolved, so the subquery returns the SAME value
    // for every outer row. The exact value depends on how the unresolved
    // column is interpreted, but the key observation is: all 5 rows get the
    // same value, which is wrong (correct result varies per dept).
    let r = rows(
        &db,
        "SELECT id, (SELECT MAX(salary) FROM emp e2 WHERE e2.dept_id = emp.dept_id) AS m \
         FROM emp ORDER BY id",
    );
    assert_eq!(r.len(), 5);
    let first_val = match &r[0][1] {
        Value::Integer(n) => Some(*n),
        Value::Null => None,
        _ => panic!("unexpected value type"),
    };
    // Every row has the same (wrong) value — proof that correlation is broken.
    for (i, row) in r.iter().enumerate() {
        let this_val = match &row[1] {
            Value::Integer(n) => Some(*n),
            Value::Null => None,
            _ => panic!("row {}: unexpected value type", i),
        };
        assert_eq!(
            this_val, first_val,
            "correlated subquery currently returns same value for all rows (known bug). row {} differs: {:?}",
            i, row
        );
    }
}

#[test]
fn correlated_subquery_in_where_currently_uncorrelated() {
    let (db, _dir) = emp_db();
    // CORRECT SQL: only bob (id=2, dept 10, salary 200 > avg 150) qualifies.
    // CURRENT (BUGGY): outer ref unresolved → subquery computes global AVG
    // = (100+200+150+300)/4 = 187.5. WHERE salary > 187.5 → bob(200), eve(300).
    let r = rows(
        &db,
        "SELECT id FROM emp e \
         WHERE salary > (SELECT AVG(salary) FROM emp e2 WHERE e2.dept_id = e.dept_id)",
    );
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    let mut ids_sorted = ids.clone();
    ids_sorted.sort();
    // Document current bug: returns global-AVG-based filter, not per-dept.
    assert_eq!(ids_sorted, vec![2, 5], "correlated subquery in WHERE returns global-AVG result (known bug)");
}

/// Sanity check: UNCORRELATED subqueries (no outer reference) DO work
/// correctly. This is the baseline that any future correlation fix must
/// preserve.
#[test]
fn uncorrelated_subquery_in_where_works() {
    let (db, _dir) = emp_db();
    let r = rows(&db, "SELECT id FROM emp WHERE salary > (SELECT AVG(salary) FROM emp)");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    let mut ids_sorted = ids.clone();
    ids_sorted.sort();
    assert_eq!(ids_sorted, vec![2, 5]); // 200, 300 > 187.5
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D2: Regression — JOIN + GROUP BY with table-qualified columns
//
// Bug: `parse_column_list` (used by GROUP BY) called `parse_identifier`
// which stops at the `.` in `tbl.col`. So `... JOIN ... ON a.x = b.y
// GROUP BY a.id` was parsed as GROUP BY `a`, leaving `.id` as unexpected
// trailing input → "Multiple statements are not supported" error.
// Fixed by introducing `parse_qualified_column_name` that consumes the
// optional `.col` suffix.
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn join_inner_group_by_qualified_column() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "SELECT a.dept_id, COUNT(*) FROM emp a INNER JOIN dept ON a.dept_id = dept.id \
         GROUP BY a.dept_id ORDER BY a.dept_id",
    );
    // depts 10, 20 (NULL excluded by INNER JOIN). Each has 2 employees.
    assert_eq!(r.len(), 2);
    assert!(matches!(&r[0][0], Value::Integer(10)));
    assert!(matches!(&r[0][1], Value::Integer(2)));
    assert!(matches!(&r[1][0], Value::Integer(20)));
    assert!(matches!(&r[1][1], Value::Integer(2)));
}

#[test]
fn join_left_group_by_qualified_column() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "SELECT a.id FROM emp a LEFT JOIN emp b ON a.dept_id = b.dept_id \
         GROUP BY a.id ORDER BY a.id",
    );
    // All 5 ids.
    assert_eq!(r.len(), 5);
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[4][0], Value::Integer(5)));
}

#[test]
fn group_by_multiple_qualified_columns() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "SELECT a.dept_id, a.salary FROM emp a GROUP BY a.dept_id, a.salary ORDER BY a.dept_id, a.salary",
    );
    // Distinct (dept, salary) combos. Note NULL salary is one group.
    assert!(r.len() >= 4);
}

#[test]
fn group_by_qualified_column_with_having() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "SELECT a.dept_id, COUNT(*) FROM emp a GROUP BY a.dept_id HAVING COUNT(*) >= 2 ORDER BY a.dept_id",
    );
    // dept 10 (2), dept 20 (2). NULL dept (1) excluded. So 2 groups.
    assert_eq!(r.len(), 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: Self-join
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn self_join_pairs_in_same_dept() {
    let (db, _dir) = emp_db();
    let r = rows(
        &db,
        "SELECT a.id, b.id FROM emp a JOIN emp b ON a.dept_id = b.dept_id AND a.id < b.id \
         ORDER BY a.id, b.id",
    );
    // dept 10: (1,2). dept 20: (3,4). NULL dept: only eve (1 row).
    assert_eq!(r.len(), 2);
}

#[test]
fn self_join_with_aggregate() {
    let (db, _dir) = emp_db();
    // For each emp, count how many earn less in same dept.
    let r = rows(
        &db,
        "SELECT a.id, COUNT(b.id) AS c FROM emp a LEFT JOIN emp b \
         ON a.dept_id = b.dept_id AND a.salary > b.salary \
         GROUP BY a.id ORDER BY a.id",
    );
    // alice(100): bob(200)>alice? no. carol(150) diff dept. → 0.
    // bob(200): alice(100)<200? yes. → 1.
    // carol(150): dave(NULL) skipped. → 0.
    // dave(NULL): a.salary > b.salary when a.salary is NULL → unknown → 0.
    // eve(300): only one in NULL dept → 0.
    let counts: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.get(1) {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(counts, vec![0, 1, 0, 0, 0]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: DELETE/UPDATE edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_where_null_no_match() {
    let (db, _dir) = emp_db();
    let result = db.execute("DELETE FROM emp WHERE salary = NULL");
    // Should delete 0 rows (NULL comparison is unknown), not all NULL rows.
    assert!(result.is_ok());
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM emp");
    assert_eq!(n, 5, "DELETE WHERE salary = NULL must delete nothing");
}

#[test]
fn delete_where_is_null() {
    let (db, _dir) = emp_db();
    db.execute("DELETE FROM emp WHERE salary IS NULL").unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM emp");
    assert_eq!(n, 4, "dave (NULL salary) should be deleted");
}

#[test]
fn update_set_null_explicit() {
    let (db, _dir) = emp_db();
    db.execute("UPDATE emp SET salary = NULL WHERE id = 1").unwrap();
    assert!(scalar_is_null(&db, "SELECT salary FROM emp WHERE id = 1"));
}

#[test]
fn update_where_no_match_zero_rows() {
    let (db, _dir) = emp_db();
    db.execute("UPDATE emp SET salary = 0 WHERE id = 99999").unwrap();
    // No row should have changed.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM emp WHERE salary = 0");
    assert_eq!(n, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Type boundary — large integers, BIGINT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bigint_storage() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, big BIGINT)");
    exec(&db, "INSERT INTO t VALUES (1, 9223372036854775807)"); // i64::MAX
    let r = rows(&db, "SELECT big FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(n) if *n == i64::MAX));
}

#[test]
fn bigint_sum_overflow_returns_correct() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, big BIGINT)");
    exec(&db, "INSERT INTO t VALUES (1, 9223372036854775806), (2, 1)");
    // SUM = i64::MAX. Should not overflow silently.
    let r = rows(&db, "SELECT SUM(big) FROM t");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, i64::MAX),
        Value::Float(f) => assert!((f - (i64::MAX as f64)).abs() < 1e10),
        o => panic!("unexpected: {:?}", o),
    }
}

#[test]
fn negative_int_arithmetic() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -10), (2, -20)");
    let n = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(n, -30);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: ORDER BY with NULLs
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_null_sorting_consistent() {
    let (db, _dir) = emp_db();
    // ORDER BY salary ASC. NULL handling varies by dialect (some first, some last).
    // Just verify the result is stable across runs.
    let mut prev: Vec<i64> = Vec::new();
    for _ in 0..5 {
        let r = rows(&db, "SELECT id FROM emp ORDER BY salary ASC");
        let ids: Vec<i64> = r
            .iter()
            .filter_map(|row| match row.first() {
                Some(Value::Integer(n)) => Some(*n),
                _ => None,
            })
            .collect();
        if prev.is_empty() {
            prev = ids;
        } else {
            assert_eq!(ids, prev, "ORDER BY salary must be deterministic");
        }
    }
}

#[test]
fn order_by_desc_with_null() {
    let (db, _dir) = emp_db();
    let r1 = rows(&db, "SELECT id FROM emp ORDER BY salary DESC");
    let r2 = rows(&db, "SELECT id FROM emp ORDER BY salary DESC");
    assert_eq!(r1, r2, "ORDER BY DESC must be deterministic");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: UNION semantics
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn union_dedup_returns_distinct_rows() {
    let (db, _dir) = emp_db();
    // UNION dedupes: {10,10,20,20} UNION {10,10,20,20} → {10,20}.
    let r = rows(
        &db,
        "SELECT dept_id FROM emp WHERE dept_id IS NOT NULL \
         UNION \
         SELECT dept_id FROM emp WHERE dept_id IS NOT NULL \
         ORDER BY dept_id",
    );
    assert_eq!(r.len(), 2, "UNION should dedupe to 2 distinct values");
    assert!(matches!(&r[0][0], Value::Integer(10)));
    assert!(matches!(&r[1][0], Value::Integer(20)));
}

#[test]
fn union_in_derived_table_not_supported() {
    // v0.5.x parse_table_ref for derived tables only accepts a single SELECT,
    // not a UNION. Document this.
    let (db, _dir) = emp_db();
    let result = db.execute(
        "SELECT COUNT(*) FROM (SELECT dept_id FROM emp \
         UNION SELECT dept_id FROM dept) AS u",
    );
    assert!(
        result.is_err(),
        "UNION inside derived table is not supported in v0.5.x"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: String functions edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn upper_lower_functions() {
    let (db, _dir) = emp_db();
    let r = rows(&db, "SELECT UPPER(name) FROM emp WHERE id = 1");
    match &r[0][0] {
        Value::Text(t) => assert_eq!(t.as_str(), "ALICE"),
        o => panic!("{:?}", o),
    }
    let r = rows(&db, "SELECT LOWER(name) FROM emp WHERE id = 1");
    match &r[0][0] {
        Value::Text(t) => assert_eq!(t.as_str(), "alice"),
        o => panic!("{:?}", o),
    }
}

#[test]
fn length_function() {
    let (db, _dir) = emp_db();
    let n = scalar_i64(&db, "SELECT LENGTH(name) FROM emp WHERE id = 1");
    assert_eq!(n, 5); // "alice"
}

#[test]
fn abs_function() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, -42)");
    let n = scalar_i64(&db, "SELECT ABS(v) FROM t WHERE id = 1");
    assert_eq!(n, 42);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: INSERT edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_duplicate_pk_errors() {
    let (db, _dir) = emp_db();
    let result = db.execute("INSERT INTO emp VALUES (1, 'dupe', 10, 999)");
    assert!(result.is_err(), "duplicate PK must error");
}

#[test]
fn insert_explicit_null() {
    let (db, _dir) = emp_db();
    db.execute("INSERT INTO emp VALUES (99, 'x', NULL, NULL)").unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM emp WHERE salary IS NULL AND dept_id IS NULL");
    assert_eq!(n, 1);
}

#[test]
fn insert_missing_column_uses_null() {
    let (db, _dir) = emp_db();
    // INSERT with fewer values than columns - should error in strict mode,
    // or fill missing with NULL/defaults.
    let result = db.execute("INSERT INTO emp (id, name) VALUES (98, 'partial')");
    if result.is_ok() {
        let r = rows(&db, "SELECT salary FROM emp WHERE id = 98");
        // Missing columns should be NULL.
        assert!(matches!(r[0][0], Value::Null));
    }
    // Else: error is acceptable.
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: Reopen persistence
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_preserves_null_data() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        exec(&db, "INSERT INTO t VALUES (1, NULL), (2, 100)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert!(scalar_is_null(&db, "SELECT v FROM t WHERE id = 1"));
    let n = scalar_i64(&db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(n, 100);
}
