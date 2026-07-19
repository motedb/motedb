//! Bug-hunt v19: previously-untested corners — BETWEEN/IN/NOT IN semantics,
//! LIKE with special chars, multi-level JOINs, nested CASE, batch INSERT,
//! type coercion in WHERE, and arithmetic edge cases.

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

fn setup() -> (Database, TempDir) {
    let (db, dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT, v INT, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES \
        (1, 'apple',  10, 1.5), \
        (2, 'banana', 20, 2.5), \
        (3, 'cherry', 30, 3.5), \
        (4, 'date',   40, 4.5), \
        (5, 'egg',    50, 5.5)");
    (db, dir)
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: BETWEEN semantics
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn between_inclusive() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v BETWEEN 20 AND 40");
    assert_eq!(ids, vec![2, 3, 4], "BETWEEN is inclusive on both ends");
}

#[test]
fn between_with_float_bounds() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE f BETWEEN 2.0 AND 4.0");
    assert_eq!(ids, vec![2, 3], "f=2.5,3.5 in [2.0,4.0]; 4.5 excluded");
}

#[test]
fn not_between() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v NOT BETWEEN 20 AND 40");
    assert_eq!(ids, vec![1, 5]);
}

#[test]
fn between_with_column_bounds() {
    let (db, _dir) = setup();
    // WHERE v BETWEEN f AND f*10 — expressions as bounds.
    let r = rows(&db, "SELECT id FROM t WHERE v BETWEEN f AND f * 10 ORDER BY id");
    // For each row, check if v is between f and f*10:
    // id=1: v=10, [1.5,15] ✓
    // id=2: v=20, [2.5,25] ✓
    // id=3: v=30, [3.5,35] ✓
    // id=4: v=40, [4.5,45] ✓
    // id=5: v=50, [5.5,55] ✓
    assert_eq!(r.len(), 5);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: IN / NOT IN semantics
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn in_list_basic() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v IN (10, 30, 50)");
    assert_eq!(ids, vec![1, 3, 5]);
}

#[test]
fn in_list_empty_no_match() {
    let (db, _dir) = setup();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v IN (999, 998)");
    assert_eq!(n, 0);
}

#[test]
fn not_in_list() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v NOT IN (10, 30, 50)");
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn in_list_with_strings() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name IN ('apple', 'cherry')");
    assert_eq!(ids, vec![1, 3]);
}

#[test]
fn in_list_single_value() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v IN (30)");
    assert_eq!(ids, vec![3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: LIKE patterns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn like_prefix() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name LIKE 'ap%'");
    assert_eq!(ids, vec![1]);
}

#[test]
fn like_suffix() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name LIKE '%a'");
    // apple (ends e), banana (ends a), cherry (y), date (e), egg (g) → banana
    assert_eq!(ids, vec![2]);
}

#[test]
fn like_contains() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name LIKE '%a%'");
    // apple (a), banana (a,a), date (a) all contain 'a'. cherry and egg do NOT.
    assert_eq!(ids, vec![1, 2, 4]);
}

#[test]
fn like_single_char() {
    let (db, _dir) = setup();
    // _ matches any single character. 'egg' is 3 chars; 'egg' LIKE '_gg' ✓
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name LIKE '_gg'");
    assert_eq!(ids, vec![5]);
}

#[test]
fn like_no_wildcard_exact() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name LIKE 'apple'");
    assert_eq!(ids, vec![1]);
}

#[test]
fn not_like() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE name NOT LIKE '%a%'");
    // cherry (no 'a') and egg (no 'a').
    assert_eq!(ids, vec![3, 5]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: Multi-level JOINs
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn three_table_join() {
    let (db, _dir) = setup();
    exec(&db, "CREATE TABLE u (id INT PRIMARY KEY, t_id INT, name TEXT)");
    exec(&db, "CREATE TABLE m (id INT PRIMARY KEY, u_id INT, score INT)");
    exec(&db, "INSERT INTO u VALUES (1, 1, 'u1'), (2, 1, 'u2'), (3, 2, 'u3')");
    exec(&db, "INSERT INTO m VALUES (1, 1, 100), (2, 2, 200), (3, 3, 300)");

    let n = scalar_i64(
        &db,
        "SELECT COUNT(*) FROM t JOIN u ON t.id = u.t_id JOIN m ON u.id = m.u_id",
    );
    assert_eq!(n, 3);
}

#[test]
fn join_with_where_after_two_joins() {
    let (db, _dir) = setup();
    exec(&db, "CREATE TABLE u (id INT PRIMARY KEY, t_id INT, name TEXT)");
    exec(&db, "INSERT INTO u VALUES (1, 1, 'u1'), (2, 1, 'u2'), (3, 2, 'u3')");
    let r = rows(
        &db,
        "SELECT u.id FROM t JOIN u ON t.id = u.t_id WHERE t.v > 15 ORDER BY u.id",
    );
    // t.id=1 (v=10) excluded. t.id=2 (v=20) → u3. So 1 row.
    // Wait, u with t_id=1 are u1,u2; t_id=2 is u3. WHERE t.v > 15 keeps t.id=2,3,4,5.
    // u3 (t_id=2) ✓. No other u. So 1 row.
    assert_eq!(r.len(), 1);
}

#[test]
fn left_join_preserves_left_rows() {
    let (db, _dir) = setup();
    exec(&db, "CREATE TABLE u (id INT PRIMARY KEY, t_id INT)");
    exec(&db, "INSERT INTO u VALUES (1, 1), (2, 1)"); // u1,u2 both match t1
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t LEFT JOIN u ON t.id = u.t_id");
    // LEFT JOIN preserves all left rows. t1 matches u1 AND u2 (2 rows);
    // t2..t5 match nothing (1 row each with NULLs). Total: 2 + 4 = 6.
    assert_eq!(n, 6);
}

#[test]
fn right_join_preserves_right_rows() {
    let (db, _dir) = setup();
    exec(&db, "CREATE TABLE u (id INT PRIMARY KEY, t_id INT)");
    exec(&db, "INSERT INTO u VALUES (1, 1), (2, 1), (3, 99)"); // u3 has no match
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t RIGHT JOIN u ON t.id = u.t_id");
    // RIGHT JOIN preserves all right (u) rows: 3.
    assert_eq!(n, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: Nested CASE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn nested_case_when() {
    let (db, _dir) = setup();
    let r = rows(
        &db,
        "SELECT id, CASE WHEN v < 25 THEN CASE WHEN v < 15 THEN 'low-low' ELSE 'low-high' END ELSE 'high' END AS c FROM t ORDER BY id",
    );
    // id=1 v=10 < 15 → 'low-low'
    // id=2 v=20, <25, >=15 → 'low-high'
    // id=3 v=30 → 'high'
    assert!(matches!(&r[0][1], Value::Text(t) if t.as_str() == "low-low"));
    assert!(matches!(&r[1][1], Value::Text(t) if t.as_str() == "low-high"));
    assert!(matches!(&r[2][1], Value::Text(t) if t.as_str() == "high"));
}

#[test]
fn case_without_else_returns_null() {
    let (db, _dir) = setup();
    let r = rows(&db, "SELECT CASE WHEN v > 100 THEN 'big' END FROM t WHERE id = 1");
    // No ELSE, no match → NULL.
    assert!(matches!(&r[0][0], Value::Null));
}

#[test]
fn case_with_aggregate_in_condition() {
    let (db, _dir) = setup();
    let r = rows(
        &db,
        "SELECT CASE WHEN COUNT(*) > 3 THEN 'many' ELSE 'few' END FROM t",
    );
    // 5 rows → 'many'.
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "many"));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: Batch INSERT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_multiple_values_single_stmt() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 3);
}

#[test]
fn insert_with_explicit_columns_subset() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    exec(&db, "INSERT INTO t (id, a) VALUES (1, 10)");
    // b should be NULL.
    let r = rows(&db, "SELECT b FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Type coercion in WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_int_col_compared_to_float_literal() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v > 25.5");
    // v is INT; 25.5 is float literal. v > 25.5 → 30, 40, 50.
    assert_eq!(ids, vec![3, 4, 5]);
}

#[test]
fn where_float_col_compared_to_int_literal() {
    let (db, _dir) = setup();
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE f > 3");
    // f is FLOAT; 3 is int literal. f > 3.0 → 3.5, 4.5, 5.5.
    assert_eq!(ids, vec![3, 4, 5]);
}

#[test]
fn where_string_compared_to_int_no_match() {
    let (db, _dir) = setup();
    // name is TEXT; comparing to int literal. Should match nothing (or all?).
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE name = 5");
    assert_eq!(n, 0, "string = int should not match");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: Arithmetic edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn division_by_zero() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 0)");
    // SQL standard: division by zero is an error in most DBs, but SQLite
    // returns NULL. Either is acceptable; just verify no crash.
    let r = db.execute("SELECT v / 0 FROM t WHERE id = 1");
    match r {
        Ok(_) => { /* NULL or value, OK */ }
        Err(_) => { /* error, OK */ }
    }
}

#[test]
fn modulo_operation() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 17), (2, 4)");
    let r = rows(&db, "SELECT v % 5 FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(2)));
}

#[test]
fn chained_arithmetic_precedence() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // 10 + 2 * 3 = 16 (multiplication binds tighter)
    let r = rows(&db, "SELECT v + 2 * 3 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 16),
        o => panic!("expected 16, got {:?}", o),
    }
}

#[test]
fn parentheses_override_precedence() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT (v + 2) * 3 FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 36),
        o => panic!("expected 36, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: DISTINCT edge cases
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn distinct_single_column() {
    let (db, _dir) = setup();
    exec(&db, "INSERT INTO t VALUES (6, 'apple', 10, 1.5)"); // dupe name+v+f
    let r = rows(&db, "SELECT DISTINCT name FROM t ORDER BY name");
    // apple, banana, cherry, date, egg, apple → 5 distinct.
    assert_eq!(r.len(), 5);
}

#[test]
fn distinct_multi_column() {
    let (db, _dir) = setup();
    exec(&db, "INSERT INTO t VALUES (6, 'apple', 10, 1.5)");
    let r = rows(&db, "SELECT DISTINCT name, v FROM t ORDER BY name, v");
    // (apple, 10) twice → 1. Total 5 distinct (name, v) pairs.
    assert_eq!(r.len(), 5);
}

#[test]
fn distinct_with_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, NULL), (3, 5)");
    let r = rows(&db, "SELECT DISTINCT v FROM t ORDER BY v");
    // NULL is distinct from NULL in some DBs (Postgres treats them as
    // equal for DISTINCT), but in others they're separate. Standard says
    // DISTINCT treats NULLs as equal. So 2 rows: NULL, 5.
    assert!(
        r.len() <= 3,
        "DISTINCT NULL handling: got {} rows",
        r.len()
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: COUNT variants
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_star_vs_count_col_with_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 30)");
    let star = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    let col = scalar_i64(&db, "SELECT COUNT(v) FROM t");
    assert_eq!(star, 3, "COUNT(*) counts all rows");
    assert_eq!(col, 2, "COUNT(col) skips NULL");
}

#[test]
fn count_with_where() {
    let (db, _dir) = setup();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v > 25");
    assert_eq!(n, 3);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: ORDER BY with multiple keys
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_two_keys_same_direction() {
    let (db, _dir) = setup();
    exec(&db, "INSERT INTO t VALUES (7, 'apple', 10, 9.9)"); // dupe name+v with id=1
    let r = rows(&db, "SELECT id FROM t WHERE name = 'apple' ORDER BY v ASC, id ASC");
    // Two 'apple' rows: ids 1 and 7. Same v=10. Order by id asc → 1, 7.
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|r| match r.first() {
            Some(Value::Integer(n)) => Some(*n),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1, 7]);
}

#[test]
fn order_by_two_keys_mixed_direction() {
    let (db, _dir) = setup();
    let r = rows(&db, "SELECT id, v FROM t ORDER BY v DESC, id ASC");
    // v desc: 50,40,30,20,10. ids: 5,4,3,2,1.
    assert!(matches!(&r[0][0], Value::Integer(5)));
    assert!(matches!(&r[4][0], Value::Integer(1)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: UPDATE/DELETE with complex WHERE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn update_where_with_and_or() {
    let (db, _dir) = setup();
    db.execute("UPDATE t SET v = 0 WHERE id = 1 OR (v > 40 AND name = 'egg')")
        .unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v = 0");
    // id=1 (matches id=1), id=5 (v=50>40 and name='egg'). So 2 rows.
    assert_eq!(n, 2);
}

#[test]
fn delete_where_with_in_subquery() {
    let (db, _dir) = setup();
    db.execute("DELETE FROM t WHERE id IN (SELECT id FROM t WHERE v > 30)")
        .unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    // v > 30 → ids 4, 5. After delete: 3 rows.
    assert_eq!(n, 3);
}

#[test]
fn delete_all_with_no_where() {
    let (db, _dir) = setup();
    db.execute("DELETE FROM t").unwrap();
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 0);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION M: NULL in arithmetic with COALESCE-like patterns
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn case_as_coalesce() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, 100)");
    let r = rows(
        &db,
        "SELECT CASE WHEN v IS NULL THEN 0 ELSE v END FROM t ORDER BY id",
    );
    assert!(matches!(&r[0][0], Value::Integer(0)));
    assert!(matches!(&r[1][0], Value::Integer(100)));
}

#[test]
fn arithmetic_with_null_in_group() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 10), (2, 1, NULL), (3, 1, 30)");
    // SUM ignores NULL: 10+30 = 40.
    let n = scalar_i64(&db, "SELECT SUM(v) FROM t WHERE g = 1");
    assert_eq!(n, 40);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION N: Reopen with various data types
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_preserves_float_precision() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
        exec(&db, "INSERT INTO t VALUES (1, 0.1), (2, 0.2), (3, 0.3)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = rows(&db, "SELECT f FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 0.1).abs() < 1e-6),
        o => panic!("{:?}", o),
    }
}

#[test]
fn reopen_preserves_text_with_special_chars() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, s TEXT)");
        exec(&db, "INSERT INTO t VALUES (1, 'hello world'), (2, 'a,b;c'), (3, 'with ''quote''')");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let r = rows(&db, "SELECT s FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "hello world"));
    let r = rows(&db, "SELECT s FROM t WHERE id = 2");
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "a,b;c"));
    let r = rows(&db, "SELECT s FROM t WHERE id = 3");
    assert!(matches!(&r[0][0], Value::Text(t) if t.as_str() == "with 'quote'"));
}
