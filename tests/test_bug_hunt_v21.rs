//! Bug-hunt v21: AUTO_INCREMENT edge cases, INSERT partial-column-list,
//! LIMIT 0, ORDER BY with table-qualified columns, LIKE with multiple _,
//! and aggressive NULL handling.

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
        o => panic!("expected int, got {:?}: {}", o, sql),
    }
}

fn ids_sorted(db: &Database, sql: &str) -> Vec<i64> {
    let r = rows(db, sql);
    let mut ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    ids.sort();
    ids
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: AUTO_INCREMENT edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn auto_increment_basic() {
    let (db, _dir) = new_db();
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)",
    );
    exec(&db, "INSERT INTO t (v) VALUES (10)");
    exec(&db, "INSERT INTO t (v) VALUES (20)");
    let r = rows(&db, "SELECT id, v FROM t ORDER BY id");
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Integer(10)));
    assert!(matches!(&r[1][0], Value::Integer(2)));
    assert!(matches!(&r[1][1], Value::Integer(20)));
}

#[test]
fn auto_increment_with_explicit_start() {
    let (db, _dir) = new_db();
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT = 100, v INT)",
    );
    exec(&db, "INSERT INTO t (v) VALUES (1)");
    let r = rows(&db, "SELECT id FROM t");
    assert!(matches!(&r[0][0], Value::Integer(100)));
}

#[test]
fn auto_increment_skips_explicit_id() {
    let (db, _dir) = new_db();
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)",
    );
    exec(&db, "INSERT INTO t VALUES (5, 100)");
    exec(&db, "INSERT INTO t (v) VALUES (200)");
    // After explicit id=5, next auto should be at least 6.
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert!(r.len() >= 2);
    // First is 5 (explicit). Second is the auto-increment value.
    assert!(matches!(&r[0][0], Value::Integer(5)));
    match &r[1][0] {
        Value::Integer(n) => assert!(*n > 5, "auto_increment must skip explicit id"),
        o => panic!("expected int, got {:?}", o),
    }
}

#[test]
fn auto_increment_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(
            &db,
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)",
        );
        exec(&db, "INSERT INTO t (v) VALUES (1)");
        exec(&db, "INSERT INTO t (v) VALUES (2)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    exec(&db, "INSERT INTO t (v) VALUES (3)");
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    // ids 1, 2, 3.
    assert_eq!(r.len(), 3);
    assert!(matches!(&r[2][0], Value::Integer(3)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: INSERT with partial column list
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_partial_columns_first_omitted() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT, c INT)");
    // Insert into id, b, c only; a should be NULL.
    exec(&db, "INSERT INTO t (id, b, c) VALUES (1, 1, 2)");
    let r = rows(&db, "SELECT id, a, b, c FROM t");
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Null), "omitted column a should be NULL");
    assert!(matches!(&r[0][2], Value::Integer(1)));
    assert!(matches!(&r[0][3], Value::Integer(2)));
}

#[test]
fn insert_omitted_primary_key_errors() {
    // SQL standard: PRIMARY KEY columns are NOT NULL. Omitting the PK
    // in a column-list INSERT must error (NULL PK not allowed).
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    let result = db.execute("INSERT INTO t (a) VALUES (1)");
    assert!(result.is_err(), "omitted PRIMARY KEY must error");
}

#[test]
fn insert_partial_columns_reorder() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t (b, a, id) VALUES (20, 10, 1)");
    let r = rows(&db, "SELECT id, a, b FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Integer(10)));
    assert!(matches!(&r[0][2], Value::Integer(20)));
}

#[test]
fn insert_partial_columns_batch() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t (id, a) VALUES (1, 10), (2, 20), (3, 30)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 3);
    // b should be NULL in all rows.
    let r = rows(&db, "SELECT b FROM t WHERE id = 2");
    assert!(matches!(&r[0][0], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: LIMIT 0
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn limit_zero_returns_no_rows() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let r = rows(&db, "SELECT * FROM t LIMIT 0");
    assert_eq!(r.len(), 0, "LIMIT 0 returns no rows");
}

#[test]
fn limit_zero_with_order_by() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT * FROM t ORDER BY v LIMIT 0");
    assert_eq!(r.len(), 0);
}

#[test]
fn offset_larger_than_count_returns_empty() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT * FROM t OFFSET 100");
    assert_eq!(r.len(), 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: ORDER BY with table-qualified columns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_qualified_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    let r = rows(&db, "SELECT id FROM t ORDER BY t.v");
    assert!(matches!(&r[0][0], Value::Integer(2))); // v=10
    assert!(matches!(&r[1][0], Value::Integer(3))); // v=20
    assert!(matches!(&r[2][0], Value::Integer(1))); // v=30
}

#[test]
fn order_by_qualified_desc() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    let r = rows(&db, "SELECT id FROM t ORDER BY t.v DESC");
    assert!(matches!(&r[0][0], Value::Integer(1))); // v=30
    assert!(matches!(&r[2][0], Value::Integer(2))); // v=10
}

#[test]
fn order_by_alias_of_expression() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT id, v * 2 AS dbl FROM t ORDER BY dbl");
    assert!(matches!(&r[0][0], Value::Integer(1))); // dbl=20
    assert!(matches!(&r[1][0], Value::Integer(2))); // dbl=40
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: LIKE with multiple underscore wildcards
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_two_underscores() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 'ab'), (2, 'abc'), (3, 'a'), (4, 'abcd')",
    );
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE s LIKE '__'");
    // '__' matches exactly 2 chars: 'ab'.
    assert_eq!(ids, vec![1]);
}

#[test]
fn like_underscore_then_literal() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'cat'), (2, 'bat'), (3, 'at'), (4, 'chat')");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE s LIKE '_at'");
    // '_at' matches 3-char strings ending in 'at': 'cat', 'bat'.
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn like_mixed_underscore_percent() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'hello'), (2, 'hxllo'), (3, 'hxxlo')");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE s LIKE 'h_llo'");
    // 'h_llo': h + 1 char + llo. 'hello' ✓, 'hxllo' ✓, 'hxxlo' ✗ (2 chars).
    assert_eq!(ids, vec![1, 2]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: Aggressive NULL handling
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn null_in_in_list_with_match() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, NULL)");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v IN (10, 20)");
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn not_in_with_null_in_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    // v NOT IN (10, 30): row 1 (10) is in list, row 3 (30) is in list.
    // Row 2 (NULL): NULL NOT IN (10,30) → unknown → not counted.
    // So 0 rows.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v NOT IN (10, 30)");
    assert_eq!(n, 0, "NOT IN with NULL column returns no rows (3-valued logic)");
}

#[test]
fn null_comparison_returns_unknown() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL)");
    // NULL > 5 is unknown, not false. So WHERE returns no rows.
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v > 5");
    assert_eq!(n, 0);
}

#[test]
fn null_in_arithmetic_yields_null_in_select() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL)");
    let r = rows(&db, "SELECT v + 100 FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Boolean operators (TRUE / FALSE)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn boolean_literal_true() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, b BOOLEAN)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE), (2, FALSE)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE b = TRUE");
    assert_eq!(n, 1);
}

#[test]
fn boolean_literal_false() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, b BOOLEAN)");
    exec(&db, "INSERT INTO t VALUES (1, TRUE), (2, FALSE)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE b = FALSE");
    assert_eq!(n, 1);
}

#[test]
fn boolean_in_case() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(
        &db,
        "SELECT CASE WHEN v > 5 THEN TRUE ELSE FALSE END FROM t WHERE id = 1",
    );
    assert!(matches!(&r[0][0], Value::Bool(true)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: Stress — many small INSERTs in one transaction
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn many_inserts_then_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    for i in 1..=50 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 2));
    }
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 50);
    let sum = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(sum, 2 * (1 + 50) * 50 / 2); // 2 * sum(1..50) = 2550
}

#[test]
fn interleaved_insert_delete() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    for i in 1..=20 {
        exec(&db, &format!("INSERT INTO t VALUES ({})", i));
    }
    // Delete even ids.
    for i in (2..=20).step_by(2) {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 10, "10 odd ids left");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: Wide tables (many columns)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn wide_table_20_columns() {
    let (db, _dir) = new_db();
    let cols: Vec<String> = (0..20).map(|i| format!("c{}", i)).collect();
    let col_defs: Vec<String> = cols.iter().map(|c| format!("{} INT", c)).collect();
    let sql = format!("CREATE TABLE t ({})", col_defs.join(", "));
    exec(&db, &sql);

    let vals: Vec<String> = (0..20).map(|i| (i * i).to_string()).collect();
    let insert = format!("INSERT INTO t VALUES ({})", vals.join(", "));
    exec(&db, &insert);

    let r = rows(&db, "SELECT c0, c19 FROM t");
    assert!(matches!(&r[0][0], Value::Integer(0)));
    assert!(matches!(&r[0][1], Value::Integer(361))); // 19^2
}

#[test]
fn wide_table_select_star() {
    let (db, _dir) = new_db();
    let cols: Vec<String> = (0..10).map(|i| format!("c{}", i)).collect();
    let col_defs: Vec<String> = cols.iter().map(|c| format!("{} INT", c)).collect();
    exec(&db, &format!("CREATE TABLE t ({})", col_defs.join(", ")));
    exec(&db, "INSERT INTO t VALUES (1,2,3,4,5,6,7,8,9,10)");
    let r = rows(&db, "SELECT * FROM t");
    assert_eq!(r[0].len(), 10);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: ORDER BY with ties (stable sort)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_with_ties_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1), (2, 1), (3, 2), (4, 2), (5, 1)");
    // 5 rows, group values: 1,1,2,2,1. ORDER BY g should give all 5.
    let r = rows(&db, "SELECT id FROM t ORDER BY g");
    assert_eq!(r.len(), 5);
}

#[test]
fn order_by_ties_with_secondary() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 30), (2, 1, 10), (3, 1, 20)");
    let r = rows(&db, "SELECT id FROM t ORDER BY g ASC, v ASC");
    // g=1 for all. Order by v: 10(id=2), 20(id=3), 30(id=1).
    assert!(matches!(&r[0][0], Value::Integer(2)));
    assert!(matches!(&r[1][0], Value::Integer(3)));
    assert!(matches!(&r[2][0], Value::Integer(1)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: HAVING without GROUP BY (aggregate filter)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn having_on_global_aggregate() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    // HAVING without GROUP BY: if condition true, return 1 row, else 0.
    let r = rows(&db, "SELECT SUM(v) FROM t HAVING SUM(v) > 50");
    assert_eq!(r.len(), 1, "HAVING true → 1 row");
    assert!(matches!(&r[0][0], Value::Integer(60)));
}

#[test]
fn having_on_global_aggregate_false() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    let r = rows(&db, "SELECT SUM(v) FROM t HAVING SUM(v) > 100");
    assert_eq!(r.len(), 0, "HAVING false → 0 rows");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: String edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn empty_string_value() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Text(t) if t.is_empty()));
}

#[test]
fn string_with_spaces() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, '   spaced   ')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "   spaced   "));
}

#[test]
fn string_with_unicode() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'héllo wörld')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "héllo wörld"));
}

#[test]
fn string_with_newline() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
    // Embed a literal newline via SQL — should be preserved.
    exec(&db, "INSERT INTO t VALUES (1, 'line1\nline2')");
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(t) => {
            assert!(t.contains("line1"));
            assert!(t.contains("line2"));
        }
        o => panic!("{:?}", o),
    }
}
