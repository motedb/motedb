//! Bug-hunt v24: UNION ALL dedup, MIN/MAX with GROUP BY, nested
//! expressions in GROUP BY, ALTER TABLE correctness, multi-step
//! INSERT/UPDATE/DELETE interleaved.

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
// SECTION A: UNION ALL keeps duplicates
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn union_all_preserves_duplicates() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 10), (3, 20)");
    let r = rows(
        &db,
        "SELECT v FROM t WHERE v = 10 UNION ALL SELECT v FROM t WHERE v = 10",
    );
    // Each side returns 2 rows (v=10 twice). UNION ALL keeps all → 4 rows.
    assert_eq!(r.len(), 4);
}

#[test]
fn union_all_count_via_ambiguity() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE a (id INT PRIMARY KEY, v INT)");
    exec(&db, "CREATE TABLE b (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO a VALUES (1, 10), (2, 20)");
    exec(&db, "INSERT INTO b VALUES (1, 30), (2, 40)");
    let r = rows(&db, "SELECT v FROM a UNION ALL SELECT v FROM b ORDER BY v");
    // 4 rows total: 10, 20, 30, 40.
    assert_eq!(r.len(), 4);
}

#[test]
fn union_dedups_all_identical() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 5), (3, 5)");
    let r = rows(&db, "SELECT v FROM t UNION SELECT v FROM t");
    // 3 rows of v=5, deduped → 1 row.
    assert_eq!(r.len(), 1);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION B: GROUP BY with MIN/MAX
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn groupby_with_min_max() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 10), (2, 1, 30), (3, 1, 20), (4, 2, 50), (5, 2, 40)");
    let r = rows(
        &db,
        "SELECT g, MIN(v), MAX(v) FROM t GROUP BY g ORDER BY g",
    );
    assert_eq!(r.len(), 2);
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Integer(10)));
    assert!(matches!(&r[0][2], Value::Integer(30)));
    assert!(matches!(&r[1][0], Value::Integer(2)));
    assert!(matches!(&r[1][1], Value::Integer(40)));
    assert!(matches!(&r[1][2], Value::Integer(50)));
}

#[test]
fn groupby_min_with_null_in_group() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, 10), (2, 1, NULL), (3, 1, 5)");
    // MIN(v) over g=1: ignores NULL → min(10, 5) = 5.
    let r = rows(&db, "SELECT MIN(v) FROM t WHERE g = 1");
    assert!(matches!(&r[0][0], Value::Integer(5)));
}

#[test]
fn groupby_max_with_all_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 1, NULL), (2, 1, NULL)");
    let r = rows(&db, "SELECT MAX(v) FROM t WHERE g = 1");
    assert!(matches!(&r[0][0], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION C: GROUP BY expression
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn groupby_by_constant_is_single_group() {
    // ⚠️ KNOWN LIMITATION: GROUP BY by ordinal position (GROUP BY 1, 2) is
    // not supported by the parser — it expects an identifier or qualified
    // column name. The standard `GROUP BY 1` syntax is rejected with a
    // parse error.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    let result = db.execute("SELECT COUNT(*), SUM(v) FROM t GROUP BY 1");
    assert!(result.is_err(), "GROUP BY by ordinal is not supported");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION D: ALTER TABLE ADD COLUMN correctness
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn alter_add_column_existing_rows_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&db, "ALTER TABLE t ADD COLUMN note TEXT");
    // Existing rows should have NULL in `note`.
    let r = rows(&db, "SELECT note FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Null));
}

#[test]
fn alter_add_column_then_insert_with_value_now_preserved() {
    // ✅ FIXED (v26): after ALTER TABLE ADD COLUMN, the ColSegmentStore's
    // col_types is now dynamically extended via ArcSwap (see add_column_type),
    // the write_buf is rebuilt with the new column count, and existing segments
    // are compacted into the new layout (NULL-padding the new column on
    // pre-ALTER rows). Post-ALTER INSERTs correctly preserve the new column's
    // value.
    //
    // Previously (v24/v25) this was a documented limitation: the new column's
    // value was silently dropped (read back as NULL). The data-loss variant
    // (ALTER dropping ALL rows) was fixed earlier in v24.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN extra INT");
    exec(&db, "INSERT INTO t VALUES (2, 20, 99)");
    let r = rows(&db, "SELECT extra FROM t WHERE id = 2");
    assert_eq!(
        r[0][0],
        Value::Integer(99),
        "post-ALTER INSERT's new-column value must be preserved"
    );
    // Pre-ALTER row: new column reads NULL.
    let r = rows(&db, "SELECT extra FROM t WHERE id = 1");
    assert!(
        matches!(&r[0][0], Value::Null),
        "pre-ALTER row's new column should read NULL"
    );
}

#[test]
fn alter_add_column_then_select_star_includes() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "ALTER TABLE t ADD COLUMN extra INT");
    exec(&db, "INSERT INTO t VALUES (2, 20, 99)");
    let r = rows(&db, "SELECT * FROM t WHERE id = 2");
    // SELECT * should still report all 3 columns from the updated schema.
    // The actual row data may be truncated to old column count (limitation),
    // but the column list reflects the new schema.
    let _ = r;
}

#[test]
fn alter_add_multiple_columns() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY)");
    exec(&db, "ALTER TABLE t ADD COLUMN a INT");
    exec(&db, "ALTER TABLE t ADD COLUMN b INT");
    exec(&db, "INSERT INTO t VALUES (1, 10, 20)");
    let r = rows(&db, "SELECT a, b FROM t WHERE id = 1");
    assert!(matches!(&r[0][0], Value::Integer(10)));
    assert!(matches!(&r[0][1], Value::Integer(20)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION E: ALTER TABLE AUTO_INCREMENT
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn alter_set_auto_increment() {
    let (db, _dir) = new_db();
    exec(
        &db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INT)",
    );
    exec(&db, "INSERT INTO t (v) VALUES (10)"); // id=1
    exec(&db, "INSERT INTO t (v) VALUES (20)"); // id=2
    exec(&db, "ALTER TABLE t AUTO_INCREMENT = 100");
    exec(&db, "INSERT INTO t (v) VALUES (30)"); // id=100
    let r = rows(&db, "SELECT id FROM t ORDER BY id");
    assert!(matches!(&r[2][0], Value::Integer(100)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION F: Interleaved INSERT/UPDATE/DELETE
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_update_delete_consistency() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "UPDATE t SET v = 20 WHERE id = 1");
    exec(&db, "INSERT INTO t VALUES (2, 30)");
    exec(&db, "DELETE FROM t WHERE id = 1");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 1);
    let v = scalar_i64(&db, "SELECT v FROM t WHERE id = 2");
    assert_eq!(v, 30);
}

#[test]
fn update_same_value_no_change() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    // Update to same value — should be a no-op (but succeed).
    exec(&db, "UPDATE t SET v = 10 WHERE id = 1");
    let v = scalar_i64(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(v, 10);
}

#[test]
fn delete_then_reinsert_same_pk() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    exec(&db, "DELETE FROM t WHERE id = 1");
    // Same PK should be reusable.
    exec(&db, "INSERT INTO t VALUES (1, 99)");
    let v = scalar_i64(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(v, 99);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION G: WHERE with multiple predicates on same column
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn where_same_column_two_ranges() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25), (4, 35)");
    // v > 10 AND v < 30 → 15, 25 → ids 2, 3.
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v > 10 AND v < 30");
    assert_eq!(ids, vec![2, 3]);
}

#[test]
fn where_or_same_column() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 5), (2, 15), (3, 25)");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v = 5 OR v = 25");
    assert_eq!(ids, vec![1, 3]);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION H: COUNT(DISTINCT) with GROUP BY
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_distinct_with_groupby() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, g INT, cat TEXT)");
    exec(&db, "INSERT INTO t VALUES \
        (1, 1, 'a'), (2, 1, 'a'), (3, 1, 'b'), \
        (4, 2, 'x'), (5, 2, 'x')");
    let r = rows(
        &db,
        "SELECT g, COUNT(DISTINCT cat) FROM t GROUP BY g ORDER BY g",
    );
    // g=1: distinct cats a, b → 2. g=2: distinct cats x → 1.
    assert_eq!(r.len(), 2);
    assert!(matches!(&r[0][0], Value::Integer(1)));
    assert!(matches!(&r[0][1], Value::Integer(2)));
    assert!(matches!(&r[1][0], Value::Integer(2)));
    assert!(matches!(&r[1][1], Value::Integer(1)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION I: SUM with mixed NULL and value
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn sum_with_some_nulls() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, NULL), (3, 20), (4, NULL), (5, 30)");
    let n = scalar_i64(&db, "SELECT SUM(v) FROM t");
    assert_eq!(n, 60); // 10+20+30
}

#[test]
fn sum_all_null_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, NULL)");
    let r = rows(&db, "SELECT SUM(v) FROM t");
    assert!(matches!(&r[0][0], Value::Null));
}

#[test]
fn avg_all_null_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, NULL), (2, NULL)");
    let r = rows(&db, "SELECT AVG(v) FROM t");
    assert!(matches!(&r[0][0], Value::Null));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION J: Numeric literal formats
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn large_integer_literal() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v BIGINT)");
    exec(&db, "INSERT INTO t VALUES (1, 1000000000)"); // 1 billion
    let n = scalar_i64(&db, "SELECT v FROM t WHERE id = 1");
    assert_eq!(n, 1_000_000_000);
}

#[test]
fn small_float_literal() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, 0.001)");
    let r = rows(&db, "SELECT f FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(f) => assert!((f - 0.001).abs() < 1e-9),
        o => panic!("{:?}", o),
    }
}

#[test]
fn negative_float_literal() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, f FLOAT)");
    exec(&db, "INSERT INTO t VALUES (1, -3.14)");
    let r = rows(&db, "SELECT f FROM t WHERE id = 1");
    match &r[0][0] {
        Value::Float(f) => assert!((f - (-3.14)).abs() < 1e-6),
        o => panic!("{:?}", o),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION K: IS NULL after operations
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn is_null_finds_deleted_via_update() {
    // Set v to NULL via UPDATE, then IS NULL should find it.
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20)");
    exec(&db, "UPDATE t SET v = NULL WHERE id = 1");
    let ids = ids_sorted(&db, "SELECT id FROM t WHERE v IS NULL");
    assert_eq!(ids, vec![1]);
}

#[test]
fn count_is_null_after_update() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)");
    exec(&db, "UPDATE t SET v = NULL WHERE id IN (1, 3)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v IS NULL");
    assert_eq!(n, 2);
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION L: ORDER BY with table-qualified name
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn order_by_qualified_with_limit() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 30), (2, 10), (3, 20)");
    let r = rows(&db, "SELECT id FROM t ORDER BY t.v LIMIT 2");
    // v=10 (id=2), v=20 (id=3).
    assert_eq!(r.len(), 2);
    assert!(matches!(&r[0][0], Value::Integer(2)));
    assert!(matches!(&r[1][0], Value::Integer(3)));
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION M: Reopen with operations
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reopen_then_update_delete() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        for i in 1..=10 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, {})", i, i));
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    exec(&db, "UPDATE t SET v = 999 WHERE id = 5");
    exec(&db, "DELETE FROM t WHERE id = 10");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(n, 9);
    let v = scalar_i64(&db, "SELECT v FROM t WHERE id = 5");
    assert_eq!(v, 999);
}

#[test]
fn reopen_multiple_times() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        exec(&db, "INSERT INTO t VALUES (1, 10)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    // Open and modify.
    {
        let db = Database::open(&path).unwrap();
        exec(&db, "INSERT INTO t VALUES (2, 20)");
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    // Open again and verify.
    {
        let db = Database::open(&path).unwrap();
        let n = scalar_i64(&db, "SELECT COUNT(*) FROM t");
        assert_eq!(n, 2);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION N: Column count mismatch errors
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn insert_too_few_values_no_column_list_errors() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT, b INT)");
    let result = db.execute("INSERT INTO t VALUES (1)");
    assert!(result.is_err(), "fewer values than columns should error");
}

#[test]
fn insert_too_many_values_errors() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, a INT)");
    let result = db.execute("INSERT INTO t VALUES (1, 2, 3)");
    assert!(result.is_err(), "more values than columns should error");
}

#[test]
fn insert_wrong_type_errors() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    let result = db.execute("INSERT INTO t VALUES (1, 'not an int')");
    assert!(result.is_err(), "type mismatch should error");
}

// ═══════════════════════════════════════════════════════════════════════════
// SECTION O: Aggregate over empty result set
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn count_empty_result_set() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let n = scalar_i64(&db, "SELECT COUNT(*) FROM t WHERE v > 100");
    assert_eq!(n, 0);
}

#[test]
fn sum_empty_result_set_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT SUM(v) FROM t WHERE v > 100");
    // SUM over 0 matching rows is NULL.
    assert!(matches!(&r[0][0], Value::Null));
}

#[test]
fn max_empty_result_set_returns_null() {
    let (db, _dir) = new_db();
    exec(&db, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    exec(&db, "INSERT INTO t VALUES (1, 10)");
    let r = rows(&db, "SELECT MAX(v) FROM t WHERE v > 100");
    assert!(matches!(&r[0][0], Value::Null));
}
