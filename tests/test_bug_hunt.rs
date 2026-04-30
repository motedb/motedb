//! Comprehensive bug-hunt test suite — catches real user-facing bugs
//! Categories: NULL semantics, UPDATE correctness, aggregates, type coercion,
//! DROP TABLE, CONCAT, IN/NOT IN, LIKE, ORDER BY, edge cases

use motedb::{Database, types::Value, sql::QueryResult};
use tempfile::TempDir;

fn setup_db(dir: &std::path::Path) -> Database {
    Database::create(dir.join("test.mote")).unwrap()
}

fn exec(db: &Database, sql: &str) -> QueryResult {
    db.execute(sql).unwrap().materialize().unwrap()
}

fn query_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match exec(db, sql) {
        QueryResult::Select { rows, .. } => rows,
        _ => vec![],
    }
}

fn query_single(db: &Database, sql: &str) -> Option<Vec<Value>> {
    query_rows(db, sql).into_iter().next()
}

// ============================================================
// SECTION A: NULL Semantics (highest user impact)
// ============================================================

#[test]
fn test_null_eq_null_is_false() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    // NULL = NULL should return no rows
    let rows = query_rows(&db, "SELECT * FROM t WHERE v = NULL");
    assert_eq!(rows.len(), 0, "NULL = NULL should match no rows");

    // Rows with actual values should still match
    let rows = query_rows(&db, "SELECT * FROM t WHERE v = 10");
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_null_comparison_operators() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 5)").unwrap();

    // NULL < 5, NULL > 5, NULL <= 5, NULL >= 5 should all return no rows
    let rows = query_rows(&db, "SELECT * FROM t WHERE v < 5");
    assert_eq!(rows.len(), 0, "NULL < 5 should match nothing");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v > 5");
    assert_eq!(rows.len(), 0, "NULL > 5 should match nothing");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v <= 5");
    assert_eq!(rows.len(), 1, "5 <= 5 should match row 2 only");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v >= 5");
    assert_eq!(rows.len(), 1, "5 >= 5 should match row 2 only");
}

#[test]
fn test_null_and_or_logic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT, w INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 1, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (3, NULL, NULL)").unwrap();

    // NULL AND TRUE should be false (NULL filtered out)
    let rows = query_rows(&db, "SELECT * FROM t WHERE v = 1 AND w = 1");
    assert_eq!(rows.len(), 1, "Only row 2 should match v=1 AND w=1");

    // NULL OR TRUE should return rows where either is true
    let rows = query_rows(&db, "SELECT * FROM t WHERE v = 1 OR w = 1");
    assert!(rows.len() >= 2, "Rows 1 and 2 should match: v=1 OR w=1, got {} rows", rows.len());
}

#[test]
fn test_not_null_is_false() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 5)").unwrap();

    // NOT on NULL column should NOT include the NULL row
    let rows = query_rows(&db, "SELECT * FROM t WHERE NOT v");
    // v=0 -> NOT 0 = true (include), v=NULL -> should be false (exclude)
    assert_eq!(rows.len(), 1, "NOT NULL should be false, only v=0 row should match");
}

#[test]
fn test_null_arithmetic_returns_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();

    // NULL + 5 should not match rows (result is NULL/false in WHERE)
    let rows = query_rows(&db, "SELECT * FROM t WHERE v + 5 > 0");
    assert_eq!(rows.len(), 1, "Only row 2 (v=10) should match v+5 > 0");
}

// ============================================================
// SECTION B: UPDATE Correctness
// ============================================================

#[test]
fn test_update_swap_columns() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();

    // Swap a and b: a should become 20, b should become 10
    db.execute("UPDATE t SET a = b, b = a WHERE id = 1").unwrap();

    let row = query_single(&db, "SELECT a, b FROM t WHERE id = 1").unwrap();
    assert_eq!(row[0], Value::Integer(20), "a should be 20 after swap");
    assert_eq!(row[1], Value::Integer(10), "b should be 10 after swap");
}

#[test]
fn test_update_with_expression() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    // v = v * 2 + 1 should give 21
    db.execute("UPDATE t SET v = v * 2 + 1 WHERE id = 1").unwrap();

    let row = query_single(&db, "SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(row[0], Value::Integer(21), "v should be 21 after update");
}

#[test]
fn test_update_multi_row_where() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, status TEXT, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'active', 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'inactive', 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'active', 30)").unwrap();

    // UPDATE only rows with status='active'
    let result = db.execute("UPDATE t SET v = v + 100 WHERE id = 1 AND status = 'active'");
    assert!(result.is_ok());
    if let QueryResult::Modification { affected_rows } = result.unwrap().materialize().unwrap() {
        assert_eq!(affected_rows, 1, "Should only update 1 row");
    }

    let rows = query_rows(&db, "SELECT id, v FROM t ORDER BY id");
    assert_eq!(rows[0][1], Value::Integer(110)); // id=1 updated
    assert_eq!(rows[1][1], Value::Integer(20));  // id=2 unchanged
    assert_eq!(rows[2][1], Value::Integer(30));  // id=3 unchanged
}

// ============================================================
// SECTION C: Aggregates
// ============================================================

#[test]
fn test_sum_all_null_returns_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    let row = query_single(&db, "SELECT SUM(v) FROM t");
    // SUM of all NULLs should be NULL, not 0
    assert!(row.is_some(), "SUM should return a row");
    assert_eq!(row.unwrap()[0], Value::Null, "SUM(all NULLs) should be NULL, not 0");
}

#[test]
fn test_sum_mixed_null_values() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let row = query_single(&db, "SELECT SUM(v) FROM t");
    assert_eq!(row.unwrap()[0], Value::Integer(30), "SUM should skip NULLs: 10+20=30");
}

#[test]
fn test_count_star_vs_count_column_with_nulls() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let row = query_single(&db, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(row[0], Value::Integer(3), "COUNT(*) should count all rows including NULLs");

    let row = query_single(&db, "SELECT COUNT(v) FROM t").unwrap();
    // COUNT(column) should skip NULLs
    assert_eq!(row[0], Value::Integer(2), "COUNT(v) should skip NULLs: 2 non-null values");
}

#[test]
fn test_avg_all_null_returns_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    let row = query_single(&db, "SELECT AVG(v) FROM t");
    assert!(row.is_some());
    assert_eq!(row.unwrap()[0], Value::Null, "AVG(all NULLs) should be NULL");
}

#[test]
fn test_min_max_with_nulls() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let row = query_single(&db, "SELECT MIN(v) FROM t").unwrap();
    assert_eq!(row[0], Value::Integer(10), "MIN should skip NULLs");

    let row = query_single(&db, "SELECT MAX(v) FROM t").unwrap();
    assert_eq!(row[0], Value::Integer(30), "MAX should skip NULLs");
}

#[test]
fn test_aggregate_on_empty_table() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();

    let row = query_single(&db, "SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(row[0], Value::Integer(0), "COUNT(*) on empty table should be 0");

    let row = query_single(&db, "SELECT SUM(v) FROM t");
    // SUM on empty table: could be NULL or 0, either is defensible
    assert!(row.is_some(), "SUM on empty table should still return a row");
}

// ============================================================
// SECTION D: Type Coercion & Equality
// ============================================================

#[test]
fn test_integer_float_equality() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 5.0)").unwrap();

    // 5 = 5.0 should match
    let rows = query_rows(&db, "SELECT * FROM t WHERE v = 5");
    assert_eq!(rows.len(), 1, "5 should equal 5.0");
}

#[test]
fn test_integer_float_comparison() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 3.5)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 5.0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 7.5)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE v > 4");
    assert_eq!(rows.len(), 2, "3.5 <= 4, 5.0 > 4, 7.5 > 4");
}

#[test]
fn test_order_by_with_nulls() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10)").unwrap();

    // ORDER BY should not panic on NULL
    let rows = query_rows(&db, "SELECT id, v FROM t ORDER BY v");
    assert_eq!(rows.len(), 3, "ORDER BY with NULLs should not lose rows");
}

// ============================================================
// SECTION E: CONCAT & String Functions
// ============================================================

#[test]
fn test_concat_with_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a TEXT, b TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello', 'world')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'hello', NULL)").unwrap();

    let rows = query_rows(&db, "SELECT CONCAT(a, b) FROM t ORDER BY id");
    assert_eq!(rows[0][0], Value::Text("helloworld".to_string()));

    // CONCAT with NULL: standard SQL returns NULL, but at minimum should not be "hellonull"
    let concat_null = &rows[1][0];
    match concat_null {
        Value::Null => { /* correct SQL behavior */ }
        Value::Text(s) => {
            assert_ne!(s, "helloNULL", "CONCAT('hello', NULL) should not be 'helloNULL'");
        }
        _ => panic!("Unexpected CONCAT result type"),
    }
}

// ============================================================
// SECTION F: IN / NOT IN
// ============================================================

#[test]
fn test_in_with_values() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE v IN (10, 30)");
    assert_eq!(rows.len(), 2, "IN should match v=10 and v=30");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v NOT IN (10, 30)");
    assert_eq!(rows.len(), 1, "NOT IN should match only v=20");
}

#[test]
fn test_in_with_null_in_list() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    // 10 IN (10, NULL) should find 10
    let rows = query_rows(&db, "SELECT * FROM t WHERE v IN (10, NULL)");
    assert!(rows.len() >= 1, "10 IN (10, NULL) should at least find 10");
}

// ============================================================
// SECTION G: LIKE
// ============================================================

#[test]
fn test_like_basic() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello world')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'hello')").unwrap();
    db.execute("INSERT INTO t VALUES (3, 'world hello')").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE v LIKE 'hello%'");
    assert_eq!(rows.len(), 2, "LIKE 'hello%' should match 2 rows");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v LIKE '%world%'");
    assert_eq!(rows.len(), 2, "LIKE '%world%' should match 2 rows");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v NOT LIKE 'hello%'");
    assert_eq!(rows.len(), 1, "NOT LIKE 'hello%' should match 1 row");
}

#[test]
fn test_like_on_null_column() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'hello')").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();

    // NULL LIKE '%' should return no rows
    let rows = query_rows(&db, "SELECT * FROM t WHERE v LIKE '%'");
    assert_eq!(rows.len(), 1, "NULL LIKE '%' should not match");

    // NULL NOT LIKE '%' should return no rows either
    let rows = query_rows(&db, "SELECT * FROM t WHERE v NOT LIKE '%'");
    assert_eq!(rows.len(), 0, "NULL NOT LIKE '%' should not include NULL row");
}

// ============================================================
// SECTION H: DROP TABLE
// ============================================================

#[test]
fn test_drop_table_removes_data() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t1 VALUES (1, 100)").unwrap();
    db.execute("INSERT INTO t1 VALUES (2, 200)").unwrap();

    // Verify data exists
    let rows = query_rows(&db, "SELECT * FROM t1");
    assert_eq!(rows.len(), 2);

    // Drop table
    db.execute("DROP TABLE t1").unwrap();

    // Create same table again — should be empty
    db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, v INT)").unwrap();
    let rows = query_rows(&db, "SELECT * FROM t1");
    assert_eq!(rows.len(), 0, "Recreated table after DROP should be empty");
}

#[test]
fn test_drop_table_then_recreate_different_schema() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 42)").unwrap();
    db.execute("DROP TABLE t").unwrap();

    // Recreate with different schema
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE id = 1");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Text("alice".to_string()),
        "New table should have clean data, no leftover from old table");
}

// ============================================================
// SECTION I: IS NULL / IS NOT NULL
// ============================================================

#[test]
fn test_is_null_is_not_null() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE v IS NULL");
    assert_eq!(rows.len(), 1, "IS NULL should match 1 row");
    assert_eq!(rows[0][0], Value::Integer(2));

    let rows = query_rows(&db, "SELECT * FROM t WHERE v IS NOT NULL");
    assert_eq!(rows.len(), 2, "IS NOT NULL should match 2 rows");
}

#[test]
fn test_is_null_in_update_where() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();

    db.execute("UPDATE t SET v = 0 WHERE v IS NULL").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows[0][1], Value::Integer(0), "NULL should be updated to 0");
    assert_eq!(rows[1][1], Value::Integer(10), "Non-NULL should be unchanged");
}

#[test]
fn test_is_null_in_delete_where() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, NULL)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();

    db.execute("DELETE FROM t WHERE v IS NULL").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1, "Only non-NULL row should remain");
    assert_eq!(rows[0][0], Value::Integer(2));
}

// ============================================================
// SECTION J: Edge Cases
// ============================================================

#[test]
fn test_delete_all_rows_then_reinsert() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'b')").unwrap();

    db.execute("DELETE FROM t WHERE id >= 1").unwrap();
    let rows = query_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 0, "All rows deleted");

    db.execute("INSERT INTO t VALUES (3, 'c')").unwrap();
    let rows = query_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1, "Re-insert after delete all should work");
    assert_eq!(rows[0][0], Value::Integer(3));
}

#[test]
fn test_update_no_matching_rows() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let result = db.execute("UPDATE t SET v = 99 WHERE id = 999").unwrap().materialize().unwrap();
    if let QueryResult::Modification { affected_rows } = result {
        assert_eq!(affected_rows, 0, "No rows should be affected");
    }
}

#[test]
fn test_delete_no_matching_rows() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();

    let result = db.execute("DELETE FROM t WHERE id = 999").unwrap().materialize().unwrap();
    if let QueryResult::Modification { affected_rows } = result {
        assert_eq!(affected_rows, 0, "No rows should be affected");
    }

    let rows = query_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1, "Row should still exist");
}

#[test]
fn test_multiple_updates_same_row() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    db.execute("UPDATE t SET v = 10 WHERE id = 1").unwrap();
    db.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
    db.execute("UPDATE t SET v = 30 WHERE id = 1").unwrap();

    let row = query_single(&db, "SELECT v FROM t WHERE id = 1").unwrap();
    assert_eq!(row[0], Value::Integer(30), "Last update should win");
}

#[test]
fn test_update_with_string_value() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'alice')").unwrap();

    db.execute("UPDATE t SET name = 'bob' WHERE id = 1").unwrap();

    let row = query_single(&db, "SELECT name FROM t WHERE id = 1").unwrap();
    assert_eq!(row[0], Value::Text("bob".to_string()));
}

#[test]
fn test_select_with_multiple_where_conditions() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10, 10)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE a = 10 AND b = 20");
    assert_eq!(rows.len(), 1, "Only row 1 should match a=10 AND b=20");

    let rows = query_rows(&db, "SELECT * FROM t WHERE a = 10 OR b = 10");
    assert_eq!(rows.len(), 3, "All 3 rows should match a=10 OR b=10");
}

#[test]
fn test_negative_numbers() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, -10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 0)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE v < 0");
    assert_eq!(rows.len(), 1, "Only negative row should match v < 0");

    let rows = query_rows(&db, "SELECT * FROM t WHERE v >= 0");
    assert_eq!(rows.len(), 2, "Non-negative rows should match v >= 0");
}

#[test]
fn test_float_values() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 3.14)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 2.71)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE v > 3.0");
    assert_eq!(rows.len(), 1, "Only 3.14 > 3.0");

    let row = query_single(&db, "SELECT SUM(v) FROM t").unwrap();
    match &row[0] {
        Value::Float(f) => {
            assert!((f - 5.85).abs() < 0.01, "SUM should be ~5.85, got {}", f);
        }
        other => panic!("SUM of floats should be Float, got {:?}", other),
    }
}

#[test]
fn test_order_by_desc() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 30)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 20)").unwrap();

    let rows = query_rows(&db, "SELECT v FROM t ORDER BY v DESC");
    assert_eq!(rows[0][0], Value::Integer(30));
    assert_eq!(rows[1][0], Value::Integer(20));
    assert_eq!(rows[2][0], Value::Integer(10));
}

#[test]
fn test_limit_and_offset() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({})", i)).unwrap();
    }

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id LIMIT 3");
    assert_eq!(rows.len(), 3, "LIMIT 3 should return 3 rows");

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id LIMIT 2 OFFSET 2");
    assert_eq!(rows.len(), 2, "LIMIT 2 OFFSET 2 should return 2 rows");
    assert_eq!(rows[0][0], Value::Integer(3));
    assert_eq!(rows[1][0], Value::Integer(4));
}

#[test]
fn test_count_distinct() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (4, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (5, 30)").unwrap();

    let row = query_single(&db, "SELECT COUNT(DISTINCT v) FROM t").unwrap();
    assert_eq!(row[0], Value::Integer(3), "COUNT(DISTINCT) should be 3 unique values: 10, 20, 30");
}

// ============================================================
// SECTION K: Prepared Statements
// ============================================================

#[test]
fn test_prepared_select_with_param() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 'one')").unwrap();
    db.execute("INSERT INTO t VALUES (2, 'two')").unwrap();

    // Execute same prepared statement twice with different params
    let r1 = db.execute_prepared("SELECT * FROM t WHERE id = ?", vec![Value::Integer(1)]).unwrap().materialize().unwrap();
    let r2 = db.execute_prepared("SELECT * FROM t WHERE id = ?", vec![Value::Integer(2)]).unwrap().materialize().unwrap();

    if let (QueryResult::Select { rows: rows1, .. },
            QueryResult::Select { rows: rows2, .. }) = (r1, r2) {
        assert_eq!(rows1.len(), 1);
        assert_eq!(rows1[0][0], Value::Integer(1));
        assert_eq!(rows2.len(), 1);
        assert_eq!(rows2[0][0], Value::Integer(2));
    } else {
        panic!("Prepared SELECT with params failed");
    }
}

#[test]
fn test_prepared_insert_then_select() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();

    for i in 1..=3 {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?)",
            vec![Value::Integer(i), Value::Text(format!("val_{}", i))],
        ).unwrap();
    }

    let rows = query_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0][1], Value::Text("val_1".to_string()));
    assert_eq!(rows[2][1], Value::Text("val_3".to_string()));
}

// ============================================================
// SECTION L: WHERE with complex expressions
// ============================================================

#[test]
fn test_where_between() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
    }

    let rows = query_rows(&db, "SELECT * FROM t WHERE v BETWEEN 30 AND 70");
    assert_eq!(rows.len(), 5, "BETWEEN 30 AND 70 should match v=30,40,50,60,70");
}

#[test]
fn test_where_with_parentheses() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10, 20)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (3, 30, 30)").unwrap();

    let rows = query_rows(&db, "SELECT * FROM t WHERE (a = 10 OR a = 20) AND b > 15");
    assert_eq!(rows.len(), 1, "Only row 1 matches (a=10 OR a=20) AND b>15");
    assert_eq!(rows[0][0], Value::Integer(1));
}

// ============================================================
// SECTION M: Multiple tables
// ============================================================

#[test]
fn test_multiple_tables_independent() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());

    db.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT)").unwrap();
    db.execute("CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)").unwrap();

    db.execute("INSERT INTO users VALUES (1, 'alice')").unwrap();
    db.execute("INSERT INTO users VALUES (2, 'bob')").unwrap();
    db.execute("INSERT INTO orders VALUES (1, 1, 100)").unwrap();
    db.execute("INSERT INTO orders VALUES (2, 1, 200)").unwrap();
    db.execute("INSERT INTO orders VALUES (3, 2, 300)").unwrap();

    let users = query_rows(&db, "SELECT * FROM users ORDER BY id");
    assert_eq!(users.len(), 2);

    let orders = query_rows(&db, "SELECT * FROM orders ORDER BY id");
    assert_eq!(orders.len(), 3);

    // Drop one table should not affect the other
    db.execute("DROP TABLE users").unwrap();

    let orders = query_rows(&db, "SELECT * FROM orders ORDER BY id");
    assert_eq!(orders.len(), 3, "Orders should still exist after dropping users");
}

#[test]
fn test_table_with_same_key_different_tables() {
    let dir = TempDir::new().unwrap();
    let db = setup_db(dir.path());

    db.execute("CREATE TABLE t1 (id INT PRIMARY KEY, v TEXT)").unwrap();
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, v TEXT)").unwrap();

    db.execute("INSERT INTO t1 VALUES (1, 't1_row1')").unwrap();
    db.execute("INSERT INTO t2 VALUES (1, 't2_row1')").unwrap();

    let r1 = query_single(&db, "SELECT v FROM t1 WHERE id = 1").unwrap();
    let r2 = query_single(&db, "SELECT v FROM t2 WHERE id = 1").unwrap();

    assert_eq!(r1[0], Value::Text("t1_row1".to_string()));
    assert_eq!(r2[0], Value::Text("t2_row1".to_string()));
}
