//! Bug-hunt v26: ALTER TABLE ADD COLUMN — full correctness.
//!
//! These tests lock down the fix for the v24/v25 documented limitation:
//! after `ALTER TABLE t ADD COLUMN ...`, a subsequent `INSERT INTO t VALUES
//! (...including new column value...)` must preserve that value. Previously
//! the value was silently dropped because the in-memory write_buf still had
//! N-1 column_buffers.
//!
//! Coverage:
//! - Core regression: INSERT after ALTER preserves new-column value.
//! - Type variants: TEXT, FLOAT, INT, BOOLEAN new columns.
//! - Multiple sequential ALTERs.
//! - ALTER then DELETE/UPDATE interleaved.
//! - ALTER before any INSERT (no pre-ALTER rows).
//! - Pre-ALTER rows: new column reads NULL (locked-in correct semantics).
//! - ALTER then reopen then INSERT (manifest recovery path).
//! - ALTER then COUNT(*) correctness.

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

fn affected(db: &Database, sql: &str) -> i64 {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap_or_else(|e| panic!("materialize failed: {}\n  err: {}", sql, e));
    match rs {
        QueryResult::Modification { affected_rows } => affected_rows as i64,
        _ => panic!("expected Modification for: {}", sql),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION A: Core regression — INSERT after ALTER preserves new-column value
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_after_alter_preserves_text_column() {
    // 🐛 v25 documented limitation, fixed in v26 via ArcSwap col_types.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT DEFAULT 'unknown'");
    // Post-ALTER INSERT with the new column — value must survive.
    exec(&db, "INSERT INTO t VALUES (2, 20, 'hello')");
    let r = rows(&db, "SELECT name FROM t WHERE id = 2");
    assert_eq!(r.len(), 1);
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "hello", "new column value must be preserved"),
        Value::Null => panic!("expected 'hello', got NULL — write_buf still N-1 columns"),
        o => panic!("expected Text, got {:?}", o),
    }
}

#[test]
fn insert_after_alter_preserves_int_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)");
    exec(&db, "INSERT INTO t VALUES (1, 'a')");
    exec(&db, "ALTER TABLE t ADD COLUMN score INT");
    exec(&db, "INSERT INTO t VALUES (2, 'b', 42)");
    let r = rows(&db, "SELECT score FROM t WHERE id = 2");
    match &r[0][0] {
        Value::Integer(n) => assert_eq!(*n, 42),
        o => panic!("expected int 42, got {:?}", o),
    }
}

#[test]
fn insert_after_alter_preserves_float_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN ratio FLOAT");
    exec(&db, "INSERT INTO t VALUES (2, 20, 3.14)");
    let r = rows(&db, "SELECT ratio FROM t WHERE id = 2");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 3.14).abs() < 1e-9, "got {}", f),
        o => panic!("expected float 3.14, got {:?}", o),
    }
}

#[test]
fn insert_after_alter_preserves_boolean_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN active BOOLEAN");
    exec(&db, "INSERT INTO t VALUES (2, 20, TRUE)");
    let r = rows(&db, "SELECT active FROM t WHERE id = 2");
    match &r[0][0] {
        Value::Bool(b) => assert_eq!(*b, true),
        o => panic!("expected Bool(true), got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: Pre-ALTER rows — new column reads NULL (locked-in semantics)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn pre_alter_rows_new_column_is_null() {
    // Existing rows don't have the new column — it must read NULL.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
    exec(&db, "INSERT INTO t VALUES (3, 30, 'c')");
    // Rows 1, 2: name is NULL. Row 3: name is 'c'.
    let r = rows(&db, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(r.len(), 3);
    assert_eq!(r[0][0], Value::Integer(1));
    assert!(matches!(r[0][1], Value::Null), "pre-ALTER row new col = NULL");
    assert!(matches!(r[1][1], Value::Null), "pre-ALTER row new col = NULL");
    match &r[2][1] {
        Value::Text(s) => assert_eq!(s.as_str(), "c"),
        o => panic!("expected Text 'c', got {:?}", o),
    }
}

#[test]
fn pre_alter_rows_select_new_col_count() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "ALTER TABLE t ADD COLUMN cat TEXT");
    exec(&db, "INSERT INTO t VALUES (4, 40, 'x')");
    exec(&db, "INSERT INTO t VALUES (5, 50, 'y')");
    // COUNT(cat) counts non-NULL → only the 2 new inserts.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(cat) FROM t"), 2);
    // COUNT(*) counts all rows.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 5);
    // SELECT WHERE cat IS NULL → the 3 pre-ALTER rows.
    let r = rows(&db, "SELECT id FROM t WHERE cat IS NULL ORDER BY id");
    let ids: Vec<i64> = r
        .iter()
        .filter_map(|row| match row[0] {
            Value::Integer(n) => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: Multiple sequential ALTERs
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn multiple_alter_add_columns() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN a TEXT");
    exec(&db, "INSERT INTO t VALUES (2, 20, 'a2')");
    exec(&db, "ALTER TABLE t ADD COLUMN b INT");
    exec(&db, "INSERT INTO t VALUES (3, 30, 'a3', 300)");
    // Verify each row's columns.
    let r = rows(&db, "SELECT id, v, a, b FROM t ORDER BY id");
    assert_eq!(r.len(), 3);
    // Row 1: pre both ALTERs → a=NULL, b=NULL
    assert!(matches!(r[0][2], Value::Null));
    assert!(matches!(r[0][3], Value::Null));
    // Row 2: after first ALTER, before second → a='a2', b=NULL
    match &r[1][2] {
        Value::Text(s) => assert_eq!(s.as_str(), "a2"),
        o => panic!("expected 'a2', got {:?}", o),
    }
    assert!(matches!(r[1][3], Value::Null), "b should be NULL for row 2");
    // Row 3: after both ALTERs → a='a3', b=300
    match &r[2][2] {
        Value::Text(s) => assert_eq!(s.as_str(), "a3"),
        o => panic!("expected 'a3', got {:?}", o),
    }
    match &r[2][3] {
        Value::Integer(n) => assert_eq!(*n, 300),
        o => panic!("expected int 300, got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: ALTER interleaved with DELETE / UPDATE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn alter_then_delete_then_insert() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
    affected(&db, "DELETE FROM t WHERE id = 1");
    exec(&db, "INSERT INTO t VALUES (3, 30, 'c3')");
    // Remaining: rows 2 (pre-ALTER, name=NULL) and 3 (post-ALTER, name='c3').
    let r = rows(&db, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(r.len(), 2);
    assert_eq!(r[0][0], Value::Integer(2));
    assert!(matches!(r[0][1], Value::Null));
    match &r[1][1] {
        Value::Text(s) => assert_eq!(s.as_str(), "c3"),
        o => panic!("expected 'c3', got {:?}", o),
    }
}

#[test]
fn alter_then_update_new_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
    // Update the new column on the pre-ALTER row.
    affected(&db, "UPDATE t SET name = 'updated' WHERE id = 1");
    let r = rows(&db, "SELECT name FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "updated"),
        Value::Null => panic!("UPDATE on new column should set value, got NULL"),
        o => panic!("expected 'updated', got {:?}", o),
    }
}

#[test]
fn alter_then_update_old_column_preserves_new() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
    exec(&db, "INSERT INTO t VALUES (1, 10, 'orig')");
    // Update an OLD column; the new column must be preserved.
    affected(&db, "UPDATE t SET v = 99 WHERE id = 1");
    let r = rows(&db, "SELECT v, name FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(99));
    match &r[0][1] {
        Value::Text(s) => assert_eq!(s.as_str(), "orig"),
        o => panic!("expected 'orig', got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: ALTER on empty table (no pre-ALTER rows)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn alter_on_empty_table_then_insert() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // ALTER before any INSERT — the store exists (created on first INSERT?
    // or lazily?). Either way, post-ALTER INSERT must work.
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
    exec(&db, "INSERT INTO t VALUES (1, 10, 'first')");
    let r = rows(&db, "SELECT name FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "first"),
        o => panic!("expected 'first', got {:?}", o),
    }
}

#[test]
fn alter_immediately_after_create() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "ALTER TABLE t ADD COLUMN a INT");
    exec(&db, "ALTER TABLE t ADD COLUMN b TEXT");
    exec(&db, "INSERT INTO t VALUES (1, 100, 'x')");
    let r = rows(&db, "SELECT id, a, b FROM t WHERE id = 1");
    assert_eq!(r[0][0], Value::Integer(1));
    assert_eq!(r[0][1], Value::Integer(100));
    match &r[0][2] {
        Value::Text(s) => assert_eq!(s.as_str(), "x"),
        o => panic!("expected 'x', got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: ALTER then reopen (manifest recovery path)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn alter_then_reopen_then_insert() {
    // Verify the fix survives a database reopen. After reopen, the store is
    // reconstructed from the live schema (N columns), so this should always
    // have worked — but lock it in to prevent regression.
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        exec(&db, "INSERT INTO t VALUES (1, 10)");
        exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
        exec(&db, "INSERT INTO t VALUES (2, 20, 'before_reopen')");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    // INSERT after reopen with new column.
    exec(&db, "INSERT INTO t VALUES (3, 30, 'after_reopen')");
    let r = rows(&db, "SELECT id, name FROM t ORDER BY id");
    assert_eq!(r.len(), 3);
    // Row 1: pre-ALTER → NULL.
    assert!(matches!(r[0][1], Value::Null));
    // Row 2: post-ALTER, pre-reopen → 'before_reopen'.
    match &r[1][1] {
        Value::Text(s) => assert_eq!(s.as_str(), "before_reopen"),
        o => panic!("expected 'before_reopen', got {:?}", o),
    }
    // Row 3: post-reopen → 'after_reopen'.
    match &r[2][1] {
        Value::Text(s) => assert_eq!(s.as_str(), "after_reopen"),
        o => panic!("expected 'after_reopen', got {:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: Aggregate correctness after ALTER
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_after_alter_and_insert() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&db, "ALTER TABLE t ADD COLUMN extra INT");
    exec(&db, "INSERT INTO t VALUES (3, 30, 300), (4, 40, 400)");
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 4);
    // SUM over an OLD column should be unaffected by ALTER.
    assert_eq!(scalar_i64(&db, "SELECT SUM(v) FROM t"), 100);
    // SUM over the NEW column: only post-ALTER rows have non-NULL.
    assert_eq!(scalar_i64(&db, "SELECT SUM(extra) FROM t"), 700);
}

#[test]
fn group_by_after_alter() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&db, "ALTER TABLE t ADD COLUMN cat TEXT");
    exec(&db, "INSERT INTO t VALUES (3, 30, 'a'), (4, 40, 'a'), (5, 50, 'b')");
    // GROUP BY the new column. Verify each group's SUM(v) regardless of row
    // order (ORDER BY cat placement of NULL is implementation-defined).
    let r = rows(&db, "SELECT cat, SUM(v) FROM t GROUP BY cat");
    assert_eq!(r.len(), 3, "should have 3 groups: NULL, 'a', 'b'");
    // Helper: find the row for a given cat (Text or NULL).
    let sum_for = |cat: Option<&str>| -> Option<i64> {
        r.iter().find_map(|row| {
            let matches = match (&row[0], cat) {
                (Value::Text(s), Some(c)) => s.as_str() == c,
                (Value::Null, None) => true,
                _ => false,
            };
            if matches {
                match row[1] {
                    Value::Integer(n) => Some(n),
                    _ => None,
                }
            } else {
                None
            }
        })
    };
    // NULL group: rows 1,2 → v=10+20=30.
    assert_eq!(sum_for(None), Some(30), "NULL group SUM should be 30");
    // 'a' group: rows 3,4 → 30+40=70.
    assert_eq!(sum_for(Some("a")), Some(70), "'a' group SUM should be 70");
    // 'b' group: row 5 → 50.
    assert_eq!(sum_for(Some("b")), Some(50), "'b' group SUM should be 50");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: Bulk insert after ALTER
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn bulk_insert_after_alter_preserves_all() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "ALTER TABLE t ADD COLUMN name TEXT");
    exec(
        &db,
        "INSERT INTO t VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c'), (4, 40, 'd')",
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 4);
    let r = rows(&db, "SELECT name FROM t ORDER BY id");
    let names: Vec<String> = r
        .iter()
        .map(|row| match &row[0] {
            Value::Text(s) => s.as_str().to_string(),
            o => panic!("expected text, got {:?}", o),
        })
        .collect();
    assert_eq!(names, vec!["a", "b", "c", "d"]);
}

#[test]
fn insert_many_then_alter_then_insert_more() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    // Pre-ALTER bulk insert (triggers segment flush at 8MB threshold for large N).
    for i in 1..=100 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i * 10));
    }
    exec(&db, "ALTER TABLE t ADD COLUMN tag TEXT");
    // Post-ALTER bulk insert.
    for i in 101..=200 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, {}, 'p{}')", i, i * 10, i));
    }
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t"), 200);
    // Verify a post-ALTER row.
    let r = rows(&db, "SELECT tag FROM t WHERE id = 150");
    match &r[0][0] {
        Value::Text(s) => assert_eq!(s.as_str(), "p150"),
        o => panic!("expected 'p150', got {:?}", o),
    }
    // All post-ALTER rows should have non-NULL tag.
    assert_eq!(
        scalar_i64(&db, "SELECT COUNT(tag) FROM t WHERE id > 100"),
        100
    );
    // All pre-ALTER rows should have NULL tag.
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE tag IS NULL"), 100);
}
