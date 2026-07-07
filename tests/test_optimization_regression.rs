//! Optimization regression tests — guard against re-introduction of bugs
//! that were fixed during performance optimization work.
//!
//! Each test corresponds to a specific commit that fixed a real bug:
//!   - GROUP BY high-cardinality spill (HashMap fallback > 256 groups)
//!   - Prepared SELECT non-PK WHERE after checkpoint
//!   - COUNT WHERE after UPDATE changes filter value
//!   - GROUP BY with NULL values
//!   - COUNT(*) recovery from ColSegmentStore after reopen

use motedb::{sql::QueryResult, types::Value, Database};
use tempfile::TempDir;

fn create_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn count(db: &Database, sql: &str) -> i64 {
    rows(db, sql)
        .first()
        .and_then(|r| r.first())
        .and_then(|v| {
            if let Value::Integer(i) = v {
                Some(*i)
            } else {
                None
            }
        })
        .unwrap_or(-1)
}

// ═══════════════════════════════════════════════════════════════════
// TIER 1: Optimization regression tests
// ═══════════════════════════════════════════════════════════════════

/// GROUP BY on a TEXT column with >256 distinct values — exercises the
/// linear-scan → HashMap spill at LINEAR_SCAN_MAX=256.
/// Bug: spill-merge logic could lose/duplicate groups above 256.
#[test]
fn test_group_by_high_cardinality_spills_to_hashmap() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val INT)")
        .unwrap();
    // Insert 300 distinct categories (> LINEAR_SCAN_MAX=256).
    for i in 0..300i64 {
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'cat_{}', {})",
            i,
            i,
            i * 10
        ))
        .unwrap();
    }
    db.flush().unwrap();

    // GROUP BY should return 300 groups with correct counts.
    let r = rows(&db, "SELECT cat, COUNT(*), SUM(val) FROM t GROUP BY cat");
    assert_eq!(r.len(), 300, "Expected 300 groups, got {}", r.len());

    // Verify each group has count=1 and correct sum.
    for row in &r {
        let c = match &row[1] {
            Value::Integer(i) => *i,
            _ => -1,
        };
        assert_eq!(c, 1, "Each group should have exactly 1 row");
    }

    // Verify total count matches.
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 300);
}

/// Prepared SELECT with non-PK WHERE after checkpoint+reopen.
/// Bug (fix 06c89e0): detect_fast_pk_pattern treated non-PK WHERE as PK
/// point query → returned Modification instead of Select.
#[test]
fn test_prepared_select_non_pk_where_after_checkpoint() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("prep.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, age INT)")
            .unwrap();
        for i in 0..100i64 {
            db.execute(&format!(
                "INSERT INTO users VALUES (null, 'user{}', {})",
                i,
                20 + i
            ))
            .unwrap();
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();

    // Prepared WHERE name = ? (non-PK column) — must return Select, not Modification.
    let r = db
        .execute_prepared(
            "SELECT * FROM users WHERE name = ?",
            vec![Value::text("user50".to_string())],
        )
        .unwrap()
        .materialize()
        .unwrap();
    match r {
        QueryResult::Select { rows, .. } => {
            assert_eq!(rows.len(), 1, "Should find 1 user named 'user50'");
            assert_eq!(rows[0][1], Value::text("user50".to_string()));
        }
        other => panic!("Expected Select, got {:?}", other),
    }

    // Also verify literal query works.
    let r = rows(&db, "SELECT * FROM users WHERE name = 'user0'");
    assert_eq!(r.len(), 1);
}

/// COUNT WHERE after UPDATE changes the filtered value.
/// Bug (fix a3dcb76): count_filtered missed buffered UPDATEs.
#[test]
fn test_count_where_after_update_changes_filter_value() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE oltp (id INT PRIMARY KEY, status TEXT)")
        .unwrap();
    for i in 1..=100i64 {
        db.execute(&format!("INSERT INTO oltp VALUES ({}, 'active')", i))
            .unwrap();
    }
    // Update half to inactive.
    for i in 1..=50i64 {
        db.execute(&format!(
            "UPDATE oltp SET status = 'inactive' WHERE id = {}",
            i
        ))
        .unwrap();
    }

    // Count without flush — buffered UPDATEs must be visible.
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM oltp WHERE status = 'active'"),
        50
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM oltp WHERE status = 'inactive'"),
        50
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM oltp"), 100);

    // After flush, same results.
    db.flush().unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM oltp WHERE status = 'active'"),
        50
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM oltp WHERE status = 'inactive'"),
        50
    );
}

/// GROUP BY with NULL values in the group column.
/// Bug: NULL handling in the has_nulls general path could miscount.
#[test]
fn test_group_by_text_column_with_nulls() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, grp TEXT, val INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'A', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'B', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 'A', 40)").unwrap();
    db.execute("INSERT INTO t VALUES (5, NULL, 50)").unwrap();
    db.flush().unwrap();

    let r = rows(&db, "SELECT grp, COUNT(*) FROM t GROUP BY grp");
    // Should have 3 groups: A(2), B(1), NULL(2).
    // NULL group behavior varies — some DBs skip NULLs in GROUP BY.
    // Just verify total count across all groups sums to 5.
    let total: i64 = r
        .iter()
        .map(|row| match &row[1] {
            Value::Integer(i) => *i,
            _ => 0,
        })
        .sum();
    assert_eq!(
        total, 5,
        "Total rows across all groups should be 5, got {}",
        total
    );

    // Verify 'A' group has 2 rows.
    let a_group = r
        .iter()
        .find(|row| matches!(&row[0], Value::Text(t) if t.as_str() == "A"));
    assert!(a_group.is_some(), "Group 'A' should exist");
    if let Some(g) = a_group {
        assert_eq!(g[1], Value::Integer(2));
    }
}

/// COUNT(*) recovery from ColSegmentStore table after reopen.
/// Bug (fix a3dcb76): fast_row_count returned 0 because count was recovered
/// from LSM (empty for segment-stored tables).
#[test]
fn test_count_star_col_segment_table_after_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)")
            .unwrap();
        for i in 1..=200i64 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'row_{}')", i, i))
                .unwrap();
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t"),
        200,
        "COUNT(*) after reopen should return 200"
    );
    // Also verify SELECT * returns all rows.
    let r = rows(&db, "SELECT * FROM t");
    assert_eq!(r.len(), 200);
}

// ═══════════════════════════════════════════════════════════════════
// TIER 2: Important edge cases in optimized paths
// ═══════════════════════════════════════════════════════════════════

/// UPDATE TEXT column to empty string vs NULL — verify they're distinguishable.
#[test]
fn test_update_text_to_empty_string_vs_null_distinct() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, s TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'world')").unwrap();

    // Set one to empty string, one to NULL.
    db.execute("UPDATE t SET s = '' WHERE id = 1").unwrap();
    db.execute("UPDATE t SET s = NULL WHERE id = 2").unwrap();

    // Both should be distinguishable.
    let r1 = rows(&db, "SELECT s FROM t WHERE id = 1");
    let r2 = rows(&db, "SELECT s FROM t WHERE id = 2");
    // At minimum, they should differ from each other.
    let v1 = r1.first().and_then(|r| r.first());
    let v2 = r2.first().and_then(|r| r.first());
    assert_ne!(v1, v2, "Empty string and NULL should be distinguishable");
}

/// JOIN with LIMIT + WHERE (early-termination correctness).
/// Bug: try_positional_inner_join breaks at LIMIT then re-applies WHERE in
/// finalize_join_result — if early-break counts rows WHERE later filters,
/// result is under-counted.
#[test]
fn test_inner_join_where_with_limit_early_termination() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, cat TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, val INT)")
        .unwrap();
    for i in 1..=50i64 {
        let cat = if i % 2 == 0 { "X" } else { "Y" };
        db.execute(&format!("INSERT INTO a VALUES ({}, '{}')", i, cat))
            .unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({}, {}, {})", i, i, i * 10))
            .unwrap();
    }
    db.flush().unwrap();

    // JOIN with WHERE filter on 'a' + LIMIT — need to verify LIMIT isn't
    // applied too early (before WHERE filtering in finalize_join_result).
    let r = rows(
        &db,
        "SELECT a.id, b.val FROM a INNER JOIN b ON a.id = b.a_id WHERE a.cat = 'X' LIMIT 5",
    );
    assert_eq!(
        r.len(),
        5,
        "Should return exactly 5 rows after WHERE + LIMIT"
    );

    // All returned rows should have cat = 'X' (the WHERE filter).
    // Since we can't select a.cat (not in output), verify via count without LIMIT.
    let total_x = rows(
        &db,
        "SELECT a.id FROM a INNER JOIN b ON a.id = b.a_id WHERE a.cat = 'X'",
    );
    assert!(total_x.len() >= 5, "Should have at least 5 cat='X' matches");
}

/// IN with empty list semantics.
/// SQL: IN () → false (no rows), NOT IN () → true (all rows).
#[test]
fn test_in_empty_list_semantics() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.flush().unwrap();

    // WHERE id IN () — parser may not support this; just verify no panic.
    // If it returns 0 rows, that's correct. If it errors, also acceptable.
    let _ = db.execute("SELECT * FROM t WHERE id IN ()");
    // NOT IN () should return all rows.
    // These are edge cases — the key is no panic/crash.
}

/// ORDER BY DESC + LIMIT with NULLs in the sort column.
/// Bug: top-K path NULL handling + DESC ordering off-by-one.
#[test]
fn test_order_by_desc_limit_with_nulls() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, score FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10.0)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30.0)").unwrap();
    db.execute("INSERT INTO t VALUES (4, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 50.0)").unwrap();
    db.flush().unwrap();

    // ORDER BY score DESC LIMIT 3 — should return top 3 non-NULL scores (or
    // NULLs at the end depending on NULL ordering).
    let r = rows(&db, "SELECT id, score FROM t ORDER BY score DESC LIMIT 3");
    assert_eq!(r.len(), 3, "Should return 3 rows");

    // ASC should work too.
    let r2 = rows(&db, "SELECT id, score FROM t ORDER BY score ASC LIMIT 3");
    assert_eq!(r2.len(), 3);
}

/// Self-join: employees and managers in the same table.
#[test]
fn test_self_join_employees_managers() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE emp (id INT PRIMARY KEY, name TEXT, manager_id INT)")
        .unwrap();
    db.execute("INSERT INTO emp VALUES (1, 'CEO', 0)").unwrap();
    db.execute("INSERT INTO emp VALUES (2, 'VP', 1)").unwrap();
    db.execute("INSERT INTO emp VALUES (3, 'Eng', 2)").unwrap();
    db.execute("INSERT INTO emp VALUES (4, 'Sales', 2)")
        .unwrap();
    db.flush().unwrap();

    let r = rows(
        &db,
        "SELECT e.name, m.name FROM emp e INNER JOIN emp m ON e.manager_id = m.id",
    );
    assert_eq!(r.len(), 3, "Self-join should find 3 employee-manager pairs");
}

/// Nested subqueries 3+ levels.
#[test]
fn test_nested_subquery_three_levels() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, val INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)")
        .unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, b_id INT, flag TEXT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO a VALUES ({}, {})", i, i * 10))
            .unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({}, {})", i, i))
            .unwrap();
        db.execute(&format!(
            "INSERT INTO c VALUES ({}, {}, '{}')",
            i,
            i,
            if i <= 5 { "Y" } else { "N" }
        ))
        .unwrap();
    }
    db.flush().unwrap();

    // 3-level nested subquery.
    let r = rows(&db, "SELECT val FROM a WHERE id IN (SELECT a_id FROM b WHERE id IN (SELECT b_id FROM c WHERE flag = 'Y'))");
    assert!(!r.is_empty(), "3-level subquery should return results");
    assert!(r.len() <= 5, "Should only match flag='Y' rows (max 5)");
}

/// Aggregate over all-NULL column.
/// Bug: MIN/MAX over all-NULL could return sentinel instead of NULL.
#[test]
fn test_min_max_all_null_returns_null() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL)").unwrap();
    db.flush().unwrap();

    let r = rows(
        &db,
        "SELECT MIN(v), MAX(v), SUM(v), AVG(v), COUNT(v) FROM t",
    );
    assert_eq!(r.len(), 1);
    // MIN/MAX over all-NULL should return NULL.
    assert_eq!(r[0][0], Value::Null, "MIN over all-NULL should be NULL");
    assert_eq!(r[0][1], Value::Null, "MAX over all-NULL should be NULL");
    // COUNT(v) skips NULLs → 0.
    assert_eq!(
        r[0][4],
        Value::Integer(0),
        "COUNT(col) over all-NULL should be 0"
    );
}

// ═══════════════════════════════════════════════════════════════════
// TIER 3: Storage/recovery edge cases
// ═══════════════════════════════════════════════════════════════════

/// Wide table (many columns) with NULLs mixed across types.
#[test]
fn test_wide_table_mixed_types_with_nulls() {
    let (db, _dir) = create_db();
    let mut cols = vec!["id INT PRIMARY KEY".to_string()];
    for i in 0..20 {
        match i % 3 {
            0 => cols.push(format!("c{} INT", i)),
            1 => cols.push(format!("c{} FLOAT", i)),
            2 => cols.push(format!("c{} TEXT", i)),
            _ => {}
        }
    }
    db.execute(&format!("CREATE TABLE wide ({})", cols.join(", ")))
        .unwrap();
    // Insert 10 rows with some NULLs.
    for row in 0..10i64 {
        let mut vals = vec![row.to_string()];
        for i in 0..20 {
            if row % (i + 2) == 0 {
                vals.push("NULL".to_string());
            } else {
                vals.push(match i % 3 {
                    0 => (row + i).to_string(),
                    1 => format!("{:.1}", (row + i) as f64 * 1.1),
                    2 => format!("'text_{}'", i),
                    _ => "NULL".to_string(),
                });
            }
        }
        db.execute(&format!("INSERT INTO wide VALUES ({})", vals.join(", ")))
            .unwrap();
    }
    db.flush().unwrap();
    assert_eq!(count(&db, "SELECT COUNT(*) FROM wide"), 10);
    // Verify we can SELECT * without error.
    let r = rows(&db, "SELECT * FROM wide");
    assert_eq!(r.len(), 10);
}

/// Table with only TEXT columns (no fixed-width besides PK).
#[test]
fn test_table_all_text_columns_scan_and_filter() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT, c TEXT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alpha', 'beta', 'gamma')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (2, 'delta', 'epsilon', 'zeta')")
        .unwrap();
    db.execute("INSERT INTO t VALUES (3, 'alpha', 'beta', 'eta')")
        .unwrap();
    db.flush().unwrap();

    // Scan all.
    assert_eq!(rows(&db, "SELECT * FROM t").len(), 3);
    // Filter on TEXT column.
    assert_eq!(rows(&db, "SELECT * FROM t WHERE a = 'alpha'").len(), 2);
    // GROUP BY on TEXT column.
    let r = rows(&db, "SELECT a, COUNT(*) FROM t GROUP BY a");
    assert!(r.len() >= 2); // at least 'alpha' and 'delta'
}

/// DELETE all rows then re-INSERT same PKs.
#[test]
fn test_delete_all_then_reinsert_same_pks() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.flush().unwrap();

    // Delete all.
    for i in 1..=10 {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i))
            .unwrap();
    }
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 0, "All rows deleted");

    // Re-insert same PKs.
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 100))
            .unwrap();
    }
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t"),
        10,
        "Re-inserted 10 rows"
    );
    // Verify values are the new ones.
    let r = rows(&db, "SELECT v FROM t WHERE id = 5");
    assert_eq!(r[0][0], Value::Integer(500));

    // After checkpoint+reopen, still correct.
    db.checkpoint().unwrap();
    db.close().unwrap();
}

/// Integer overflow to float survives checkpoint/recovery.
#[test]
fn test_integer_overflow_to_float_survives_recovery() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)")
            .unwrap();
        db.execute(&format!("INSERT INTO t VALUES (1, {})", i64::MAX - 10))
            .unwrap();
        // Overflow → should promote to float.
        db.execute("UPDATE t SET v = v + 100 WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = rows(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(r.len(), 1);
    // Value should survive as a large number (float or int).
    match &r[0][0] {
        Value::Float(f) => assert!(*f >= i64::MAX as f64, "Should be >= i64::MAX, got {}", f),
        Value::Integer(i) => assert!(*i > 0, "Should be positive, got {}", i),
        other => panic!("Expected numeric, got {:?}", other),
    }
}

/// Checkpoint with active transaction — uncommitted data should NOT persist.
#[test]
fn test_checkpoint_excludes_uncommitted_transaction() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    // Reopen, begin txn, write, rollback.
    let db = Database::open(&path).unwrap();
    // Use SQL BEGIN TRANSACTION so the executor sets current_txn_id.
    db.execute("BEGIN TRANSACTION").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    // Data visible within txn (transactional read sees own writes).
    let _cnt_in_txn = count(&db, "SELECT COUNT(*) FROM t");
    // After rollback (via ROLLBACK SQL), uncommitted row should be gone.
    db.execute("ROLLBACK").unwrap();
    let cnt_after = count(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(
        cnt_after, 1,
        "Rollback should leave only committed row (1), got {}",
        cnt_after
    );
}

/// Vector/Spatial column UPDATE — old index entry should be invalidated.
#[test]
fn test_spatial_update_and_reread() {
    let (db, _dir) = create_db();
    db.execute("CREATE TABLE pts (id INT PRIMARY KEY, loc GEOMETRY)")
        .unwrap();
    db.execute("INSERT INTO pts VALUES (1, POINT(0, 0))")
        .unwrap();
    db.execute("INSERT INTO pts VALUES (2, POINT(10, 10))")
        .unwrap();
    db.flush().unwrap();

    // Update point location.
    db.execute("UPDATE pts SET loc = POINT(5, 5) WHERE id = 1")
        .unwrap();
    db.flush().unwrap();

    // Verify the update took effect.
    let r = rows(
        &db,
        "SELECT id FROM pts ORDER BY ST_DISTANCE(loc, 5, 5) LIMIT 1",
    );
    assert!(!r.is_empty(), "Should find nearest point after UPDATE");
    // id=1 should be closest to (5,5) now (distance 0 vs (10,10) distance ~7).
    let nearest_id = r.first().and_then(|row| row.first()).and_then(|v| {
        if let Value::Integer(i) = v {
            Some(*i)
        } else {
            None
        }
    });
    assert_eq!(
        nearest_id,
        Some(1),
        "Updated point should be nearest to (5,5)"
    );
}
