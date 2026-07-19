//! Bug-hunt v17: WITH / Common Table Expression (CTE) support.
//!
//! Covers non-recursive CTEs end-to-end: parser, lexical scoping, executor
//! rewrite path (CTE name → derived table), column aliases, JOIN/UNION
//! composition, error handling, and the RECURSIVE marker (accepted
//! syntactically but self-reference is rejected with a clear error).
//!
//! All tests run in release mode. Run with:
//!   cargo test --release --test test_bug_hunt_v17_cte

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
        _ => panic!("expected Select result for: {}", sql),
    }
}

fn columns(db: &Database, sql: &str) -> Vec<String> {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap();
    match rs {
        QueryResult::Select { columns, .. } => columns,
        _ => panic!("expected Select result for: {}", sql),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "expected 1 row for: {}", sql);
    match r[0].first() {
        Some(Value::Integer(n)) => *n,
        o => panic!("expected integer, got {:?}: {}", o, sql),
    }
}

/// Setup helper: a small sales table with category + value.
fn sales_db() -> (Database, TempDir) {
    let (db, dir) = new_db();
    exec(
        &db,
        "CREATE TABLE sales (id INT PRIMARY KEY, cat TEXT, region TEXT, qty INT)",
    );
    exec(&db, "INSERT INTO sales VALUES \
        (1, 'a', 'east', 10), \
        (2, 'a', 'west', 20), \
        (3, 'b', 'east', 30), \
        (4, 'b', 'west', 40), \
        (5, 'c', 'east', 50)");
    (db, dir)
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Basic non-recursive CTE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_basic_scalar() {
    let (db, _dir) = new_db();
    let r = rows(&db, "WITH x AS (SELECT 1 AS v) SELECT v FROM x");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Integer(1)));
}

#[test]
fn cte_basic_no_alias() {
    let (db, _dir) = new_db();
    // No AS alias on the column — output name should be derivable.
    let r = rows(&db, "WITH x AS (SELECT 42) SELECT * FROM x");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Integer(42)));
}

#[test]
fn cte_from_table_filter() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH big AS (SELECT id, cat FROM sales WHERE qty > 25) \
         SELECT id FROM big ORDER BY id",
    );
    // qty > 25 → ids 3, 4, 5
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![3, 4, 5]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Multiple CTEs in one WITH clause
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_multiple_independent() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH a AS (SELECT id FROM sales WHERE cat = 'a'), \
                  b AS (SELECT id FROM sales WHERE cat = 'b') \
         SELECT id FROM a UNION SELECT id FROM b ORDER BY id",
    );
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![1, 2, 3, 4]);
}

#[test]
fn cte_chained_second_refs_first() {
    let (db, _dir) = sales_db();
    // a: sum per cat (a=30, b=70, c=50); b: filter a where sum > 35 → b, c
    let r = rows(
        &db,
        "WITH a AS (SELECT cat, SUM(qty) AS s FROM sales GROUP BY cat), \
                  b AS (SELECT cat, s FROM a WHERE s > 35) \
         SELECT cat, s FROM b ORDER BY cat",
    );
    assert_eq!(r.len(), 2); // cat 'b' (70) and 'c' (50)
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "b"));
    assert!(matches!(&r[0][1], Value::Integer(70)));
    assert!(matches!(&r[1][0], Value::Text(t) if t.as_str() == "c"));
    assert!(matches!(&r[1][1], Value::Integer(50)));
}

#[test]
fn cte_chained_three_levels() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH lvl1 AS (SELECT id, qty FROM sales WHERE region = 'east'), \
                  lvl2 AS (SELECT id FROM lvl1 WHERE qty >= 30), \
                  lvl3 AS (SELECT id FROM lvl2 WHERE id > 1) \
         SELECT id FROM lvl3 ORDER BY id",
    );
    // east: ids 1(10), 3(30), 5(50). qty>=30: 3, 5. id>1: 3, 5.
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![3, 5]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. CTE + JOIN (CTE joined to a real table, and to itself)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_join_with_real_table() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH avg_per_cat AS (SELECT cat, AVG(qty) AS a FROM sales GROUP BY cat) \
         SELECT sales.id, sales.qty, avg_per_cat.a \
         FROM sales JOIN avg_per_cat ON sales.cat = avg_per_cat.cat \
         ORDER BY sales.id",
    );
    assert_eq!(r.len(), 5);
    // cat 'a' avg = 15; ids 1, 2 have avg 15
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][2], Value::Float(_)));
}

#[test]
fn cte_self_join() {
    let (db, _dir) = sales_db();
    // Self-join on the CTE: pairs of rows in the same category.
    let r = rows(
        &db,
        "WITH x AS (SELECT id, cat FROM sales) \
         SELECT a.id, b.id FROM x a JOIN x b ON a.cat = b.cat AND a.id < b.id \
         ORDER BY a.id, b.id",
    );
    // cat 'a': (1,2). cat 'b': (3,4). Total 2 pairs.
    assert_eq!(r.len(), 2);
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Integer(2)));
    assert!(matches!(&r[1][0], Value::Integer(3)));
    assert!(matches!(&r[1][1], Value::Integer(4)));
}

#[test]
fn cte_join_with_alias() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH big AS (SELECT id, qty FROM sales WHERE qty > 15) \
         SELECT b.id FROM big b ORDER BY b.id",
    );
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![2, 3, 4, 5]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. CTE + WHERE / ORDER BY / LIMIT / GROUP BY / HAVING
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_with_where_order_limit() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id, qty FROM sales) \
         SELECT id, qty FROM x WHERE qty > 15 ORDER BY qty DESC LIMIT 2",
    );
    assert_eq!(r.len(), 2);
    // qty 50 (id 5), 40 (id 4)
    assert!(matches!(&r[0][0], Value::Integer(5)));
    assert!(matches!(&r[0][1], Value::Integer(50)));
    assert!(matches!(&r[1][0], Value::Integer(4)));
    assert!(matches!(&r[1][1], Value::Integer(40)));
}

#[test]
fn cte_with_groupby_having() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT cat, region, qty FROM sales) \
         SELECT cat, SUM(qty) FROM x GROUP BY cat HAVING SUM(qty) > 30 ORDER BY cat",
    );
    // cat sums: a=30, b=70, c=50. HAVING >30 → b, c
    assert_eq!(r.len(), 2);
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "b"));
    assert!(matches!(&r[0][1], Value::Integer(70)));
    assert!(matches!(&r[1][0], Value::Text(t) if t.as_str() == "c"));
    assert!(matches!(&r[1][1], Value::Integer(50)));
}

#[test]
fn cte_count_distinct() {
    let (db, _dir) = sales_db();
    let n = scalar_i64(
        &db,
        "WITH x AS (SELECT cat FROM sales) SELECT COUNT(DISTINCT cat) FROM x",
    );
    assert_eq!(n, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. CTE + UNION (CTE visible to both branches)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_visible_to_both_union_branches() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id FROM sales WHERE qty > 25) \
         SELECT id FROM x UNION SELECT id FROM x ORDER BY id",
    );
    // UNION dedupes; x has ids 3,4,5
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![3, 4, 5]);
}

#[test]
fn cte_union_all_preserves_dupes() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id FROM sales WHERE qty > 25) \
         SELECT id FROM x UNION ALL SELECT id FROM x ORDER BY id",
    );
    // 3,3,4,4,5,5
    assert_eq!(r.len(), 6);
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. CTE column aliases: WITH x(a, b, ...) AS (...)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_explicit_column_aliases() {
    let (db, _dir) = sales_db();
    let cols = columns(
        &db,
        "WITH x(category, total) AS (SELECT cat, SUM(qty) FROM sales GROUP BY cat) \
         SELECT category, total FROM x ORDER BY total DESC LIMIT 1",
    );
    assert_eq!(cols, vec!["category".to_string(), "total".to_string()]);
}

#[test]
fn cte_explicit_aliases_referenced_in_where() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x(category, total) AS (SELECT cat, SUM(qty) FROM sales GROUP BY cat) \
         SELECT total FROM x WHERE category = 'b'",
    );
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Integer(70)));
}

#[test]
fn cte_explicit_aliases_on_expressions() {
    let (db, _dir) = sales_db();
    // Note: "dbl" instead of "double" because `double` is a registered type
    // keyword (alias for FLOAT) and cannot be used as a column identifier.
    let r = rows(
        &db,
        "WITH x(dbl) AS (SELECT qty * 2 FROM sales WHERE id = 1) \
         SELECT dbl FROM x",
    );
    assert_eq!(r.len(), 1);
    assert!(matches!(&r[0][0], Value::Integer(20))); // 10 * 2
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. RECURSIVE keyword — accepted syntactically, but self-reference errors
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn recursive_keyword_accepted_for_non_self_ref() {
    let (db, _dir) = sales_db();
    // WITH RECURSIVE x AS (SELECT 1) — no self-reference, should still work.
    let r = rows(&db, "WITH RECURSIVE x AS (SELECT 1 AS v) SELECT v FROM x");
    assert_eq!(r.len(), 1);
    assert!(matches!(r[0][0], Value::Integer(1)));
}

#[test]
fn recursive_self_reference_rejected() {
    let (db, _dir) = sales_db();
    let result = db.execute("WITH RECURSIVE r AS (SELECT * FROM r) SELECT * FROM r");
    let err = match result {
        Ok(_) => panic!("expected error for recursive self-reference"),
        Err(e) => format!("{}", e),
    };
    assert!(
        err.contains("Recursive") || err.contains("self-reference"),
        "error should mention recursion: {}",
        err
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Error handling
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn err_with_before_insert_rejected() {
    let (db, _dir) = sales_db();
    let result = db.execute("WITH x AS (SELECT 1) INSERT INTO sales VALUES (99, 'z', 'n', 0)");
    assert!(result.is_err(), "WITH before INSERT should be rejected");
}

#[test]
fn err_with_before_update_rejected() {
    let (db, _dir) = sales_db();
    let result = db.execute("WITH x AS (SELECT 1) UPDATE sales SET qty = 0 WHERE id = 1");
    assert!(result.is_err(), "WITH before UPDATE should be rejected");
}

#[test]
fn err_undefined_cte_name_falls_back_to_table_error() {
    let (db, _dir) = sales_db();
    // "undefined_cte" is neither a CTE nor a table → should error.
    let result = db.execute("WITH x AS (SELECT 1) SELECT * FROM undefined_cte");
    assert!(result.is_err());
}

#[test]
fn err_malformed_with_no_as() {
    let (db, _dir) = sales_db();
    let result = db.execute("WITH x (SELECT 1) SELECT * FROM x");
    assert!(result.is_err(), "missing AS should be a parse error");
}

#[test]
fn err_malformed_with_no_paren() {
    let (db, _dir) = sales_db();
    let result = db.execute("WITH x AS SELECT 1 SELECT * FROM x");
    assert!(result.is_err(), "missing parentheses should be a parse error");
}

// ═══════════════════════════════════════════════════════════════════════════
// 9. v1 limitation: CTE not visible inside nested subquery in FROM
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_nested_subquery_reference_v1_limitation() {
    let (db, _dir) = sales_db();
    // `SELECT * FROM (SELECT * FROM cte)` — v1 does not rewrite inside
    // nested subqueries. This should error (cte is out of scope inside the
    // derived table).
    let result = db.execute(
        "WITH x AS (SELECT id FROM sales) \
         SELECT * FROM (SELECT * FROM x) AS sub",
    );
    assert!(
        result.is_err(),
        "v1 limitation: CTE reference inside nested subquery should error"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 10. Prepared-statement path handles CTEs
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_works_via_prepared_path() {
    let (db, _dir) = sales_db();
    // execute_prepared is the parameterized path. CTEs must work there too.
    let rs = match db.execute_prepared(
        "WITH x AS (SELECT id FROM sales WHERE qty > 25) SELECT id FROM x ORDER BY id",
        Vec::new(),
    ) {
        Ok(r) => r.materialize().expect("materialize"),
        Err(e) => panic!("prepared execute failed: {}", e),
    };
    match rs {
        QueryResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("expected Select"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 11. CTE inside an explicit transaction (read-your-writes still works)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_inside_transaction_sees_uncommitted_insert() {
    let (db, _dir) = sales_db();
    let tx = db.begin_transaction().expect("begin");
    // After begin_transaction, execute() routes through the txn context.
    let _ = db
        .execute("INSERT INTO sales VALUES (99, 'd', 'north', 999)")
        .and_then(|r| r.materialize());
    let n = scalar_i64(
        &db,
        "WITH x AS (SELECT cat FROM sales) SELECT COUNT(*) FROM x WHERE cat = 'd'",
    );
    assert_eq!(n, 1, "CTE inside txn should see uncommitted insert");
    db.rollback_transaction(tx).expect("rollback");
}

// ═══════════════════════════════════════════════════════════════════════════
// 12. CTE referenced multiple times (correctness: each ref independent)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_referenced_three_times_self_join() {
    let (db, _dir) = sales_db();
    // Self-join the CTE three times via INNER JOIN ON (always-true).
    // (Comma-separated FROM list `a, b` is implicit cross-join syntax the
    // parser doesn't accept; CROSS JOIN is also unsupported. INNER JOIN ON
    // 1=1 produces the same cartesian product.)
    let r = rows(
        &db,
        "WITH x AS (SELECT cat, SUM(qty) AS s FROM sales GROUP BY cat) \
         SELECT count(*) FROM x a JOIN x b ON 1=1 JOIN x c ON 1=1",
    );
    // 3 cats × 3 cats × 3 cats = 27
    assert!(
        matches!(&r[0][0], Value::Integer(n) if *n == 27),
        "got {:?}",
        r[0][0]
    );
}

#[test]
fn cte_with_count_in_main() {
    let (db, _dir) = sales_db();
    let n = scalar_i64(&db, "WITH x AS (SELECT id FROM sales) SELECT COUNT(*) FROM x");
    assert_eq!(n, 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// 13. CTE with CASE expression in projection
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_with_case_in_projection() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id, CASE WHEN qty >= 30 THEN 'big' ELSE 'small' END AS sz FROM sales) \
         SELECT id, sz FROM x WHERE sz = 'big' ORDER BY id",
    );
    // qty >= 30: ids 3, 4, 5
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![3, 4, 5]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 14. CTE with subquery in its WHERE (independent subquery, not a CTE ref)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_body_contains_independent_subquery() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id FROM sales WHERE qty > (SELECT AVG(qty) FROM sales)) \
         SELECT id FROM x ORDER BY id",
    );
    // AVG(qty) = (10+20+30+40+50)/5 = 30. qty > 30 → ids 4, 5
    let ids: Vec<i64> = r.iter().map(|row| match row[0] {
        Value::Integer(n) => n,
        _ => panic!(),
    }).collect();
    assert_eq!(ids, vec![4, 5]);
}

// ═══════════════════════════════════════════════════════════════════════════
// 15. CTE works after reopen (no schema pollution)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_works_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let n = scalar_i64(&db, "WITH x AS (SELECT v FROM t) SELECT SUM(v) FROM x");
    assert_eq!(n, 30);
}

// ═══════════════════════════════════════════════════════════════════════════
// 16. CTE name shadows a real table (CTE wins)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_name_shadows_table_v1_rejects() {
    let (db, _dir) = sales_db();
    // v1 limitation: a CTE whose body references a table of the same name is
    // indistinguishable from a self-recursive CTE at the rewrite stage, so it
    // is rejected. (SQL standard says the CTE should shadow the table; this
    // is a known limitation of the v1 implementation.)
    let result = db.execute(
        "WITH sales AS (SELECT id FROM sales WHERE id = 1) \
         SELECT id FROM sales",
    );
    assert!(
        result.is_err(),
        "v1: CTE name shadowing a real table should error (treated as self-reference)"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// 17. CTE used with ORDER BY on a computed expression
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_order_by_computed_expression() {
    let (db, _dir) = sales_db();
    let r = rows(
        &db,
        "WITH x AS (SELECT id, qty FROM sales) \
         SELECT id, qty FROM x ORDER BY qty * -1 LIMIT 3",
    );
    assert_eq!(r.len(), 3);
    // ORDER BY qty * -1 ascending: qty=10 → -10, qty=50 → -50.
    // Smallest negated value first → largest qty first: 50, 40, 30.
    assert!(matches!(&r[0][1], Value::Integer(50)));
    assert!(matches!(&r[1][1], Value::Integer(40)));
    assert!(matches!(&r[2][1], Value::Integer(30)));
}

// ═══════════════════════════════════════════════════════════════════════════
// 18. CTE in prepared statement (parameterized)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cte_in_prepared_statement_with_param() {
    let (db, _dir) = sales_db();
    // Parameterized query that uses a CTE.
    let rs = match db.execute_prepared(
        "WITH x AS (SELECT id, qty FROM sales WHERE qty >= ?) SELECT id FROM x ORDER BY id",
        vec![Value::Integer(30)],
    ) {
        Ok(r) => r.materialize().expect("materialize"),
        Err(e) => panic!("prepared exec failed: {}", e),
    };
    match rs {
        QueryResult::Select { rows, .. } => {
            // qty >= 30 → ids 3, 4, 5
            assert_eq!(rows.len(), 3);
        }
        _ => panic!("expected Select"),
    }
}
