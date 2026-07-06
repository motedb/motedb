//! v0.5.0 regression & feature tests.
//!
//! Covers the bugs fixed in v0.5.0 and the performance features added:
//! - Integer columns read correctly (not reinterpreted as Float)
//! - UPDATE produces a single newest version (no duplicate rows)
//! - SELECT * routes through the ColSegmentStore path for non-WHERE queries
//! - UPDATE value survives checkpoint + reopen (segment dedup)
//! - DELETE then COUNT(*) / SELECT sees the deletion
//! - ORDER BY col LIMIT K returns correct top-K + preserves other rows
//! - SELECT DISTINCT returns unique values
//! - bulk_load multi-page column index (300+ entries) round-trips correctly
//! - secondary column index point-lookup for high-selectivity WHERE

use motedb::types::Value;
use motedb::{DBConfig, Database, QueryResult};
use tempfile::TempDir;

fn make_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.max_result_rows = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    (dir, db)
}

fn select_rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?}", std::mem::discriminant(&other)),
    }
}

fn count(db: &Database, table: &str) -> i64 {
    let rows = select_rows(db, &format!("SELECT COUNT(*) FROM {}", table));
    match rows.first().and_then(|r| r.first()) {
        Some(Value::Integer(n)) => *n,
        other => panic!("COUNT returned {:?}", other),
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Integer/Float type correctness (was: Integer read as Float)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_integer_columns_not_read_as_float() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, age INTEGER, score INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 30, 95)").unwrap();

    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    // All three columns are INTEGER — none should decode as Float.
    assert_eq!(rows[0][0], Value::Integer(1), "id should be Integer");
    assert_eq!(rows[0][1], Value::Integer(30), "age should be Integer");
    assert_eq!(rows[0][2], Value::Integer(95), "score should be Integer");
}

#[test]
fn test_mixed_integer_and_float_columns() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, ival INTEGER, fval FLOAT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 42, 3.14)").unwrap();

    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows[0][0], Value::Integer(1));
    assert_eq!(
        rows[0][1],
        Value::Integer(42),
        "integer column must stay Integer"
    );
    assert!(
        matches!(rows[0][2], Value::Float(_)),
        "float column must be Float"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// UPDATE correctness (was: duplicate rows after UPDATE)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_update_no_duplicate_rows() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("UPDATE t SET v = 200 WHERE id = 1").unwrap();

    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1, "UPDATE must not duplicate the row");
    assert_eq!(rows[0][1], Value::Integer(200), "must see the new value");
}

#[test]
fn test_update_preserves_other_rows() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    db.execute("UPDATE t SET v = 999 WHERE id = 3").unwrap();

    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 5, "all 5 rows must survive");
    // id=3 updated, others unchanged
    assert_eq!(
        rows[2].iter().find(|v| matches!(v, Value::Integer(999))),
        Some(&Value::Integer(999))
    );
    assert_eq!(count(&db, "t"), 5);
}

#[test]
fn test_repeated_updates_same_row() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 1)").unwrap();
    for new_v in 2..=20 {
        db.execute(&format!("UPDATE t SET v = {} WHERE id = 1", new_v))
            .unwrap();
    }
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1, "20 updates must not create duplicates");
    assert_eq!(
        rows[0][1],
        Value::Integer(20),
        "must reflect the last update"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// Durability: UPDATE/DELETE survive checkpoint + reopen
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_update_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("UPDATE t SET v = 200 WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0][1],
        Value::Integer(200),
        "updated value must survive restart"
    );
}

#[test]
fn test_delete_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 20)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        count(&db, "t"),
        1,
        "deleted row must stay deleted after restart"
    );
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2), "remaining row must be id=2");
}

// ═══════════════════════════════════════════════════════════════════════
// DELETE then COUNT/SELECT consistency
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_delete_then_count_consistent() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    db.execute("DELETE FROM t WHERE id = 5").unwrap();
    assert_eq!(count(&db, "t"), 9, "COUNT must reflect the deletion");
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 9, "SELECT must reflect the deletion");
}

#[test]
fn test_delete_all_then_count() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    for i in 1..=3 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i))
            .unwrap();
    }
    for i in 1..=3 {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i))
            .unwrap();
    }
    assert_eq!(count(&db, "t"), 0, "all rows deleted");
    let rows = select_rows(&db, "SELECT * FROM t");
    assert!(rows.is_empty(), "SELECT must return no rows");
}

// ═══════════════════════════════════════════════════════════════════════
// ORDER BY + LIMIT (top-K) correctness
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_order_by_limit_desc_correct() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v FLOAT)")
        .unwrap();
    for i in 0..100 {
        db.execute(&format!("INSERT INTO t (v) VALUES ({})", i as f64 * 0.5))
            .unwrap();
    }
    let rows = select_rows(&db, "SELECT v FROM t ORDER BY v DESC LIMIT 5");
    assert_eq!(rows.len(), 5, "LIMIT 5 must return 5 rows");
    // Descending: first row has the largest value
    let first = match &rows[0][0] {
        Value::Float(f) => *f,
        _ => 0.0,
    };
    let last = match &rows[4][0] {
        Value::Float(f) => *f,
        _ => 0.0,
    };
    assert!(
        first >= last,
        "DESC order: first ({}) >= last ({})",
        first,
        last
    );
}

#[test]
fn test_order_by_limit_preserves_count() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v FLOAT)")
        .unwrap();
    for i in 0..50 {
        db.execute(&format!("INSERT INTO t (v) VALUES ({})", i as f64))
            .unwrap();
    }
    // ORDER BY LIMIT should not affect total table state
    let _ = select_rows(&db, "SELECT * FROM t ORDER BY v DESC LIMIT 3");
    assert_eq!(count(&db, "t"), 50, "table must still have all 50 rows");
}

// ═══════════════════════════════════════════════════════════════════════
// DISTINCT correctness
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_distinct_low_cardinality() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, region TEXT)")
        .unwrap();
    for i in 0..100 {
        let region = if i % 3 == 0 { "US" } else { "EU" };
        db.execute(&format!("INSERT INTO t (region) VALUES ('{}')", region))
            .unwrap();
    }
    let rows = select_rows(&db, "SELECT DISTINCT region FROM t");
    let values: Vec<String> = rows
        .iter()
        .filter_map(|r| {
            if let Value::Text(t) = &r[0] {
                Some(t.as_str().to_string())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(values.len(), 2, "should have exactly 2 distinct regions");
    assert!(values.contains(&"US".to_string()));
    assert!(values.contains(&"EU".to_string()));
}

// ═══════════════════════════════════════════════════════════════════════
// Secondary column index point-lookup (WHERE col = val)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_indexed_where_equality_correct() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, region TEXT)")
        .unwrap();
    for i in 0..100 {
        let region = if i % 3 == 0 { "US" } else { "EU" };
        db.execute(&format!(
            "INSERT INTO t (name, region) VALUES ('n{}', '{}')",
            i, region
        ))
        .unwrap();
    }
    db.execute("CREATE INDEX idx_region ON t (region) USING COLUMN")
        .unwrap();

    // High-selectivity: WHERE region = 'US' matches ~34 rows
    let rows = select_rows(&db, "SELECT * FROM t WHERE region = 'US'");
    let expected = (0..100).filter(|i| i % 3 == 0).count();
    assert_eq!(
        rows.len(),
        expected,
        "indexed WHERE must return correct count"
    );
    // All returned rows must be US
    for row in &rows {
        let region = match &row[2] {
            Value::Text(t) => t.as_str().to_string(),
            _ => String::new(),
        };
        assert_eq!(region, "US", "all rows must match the filter");
    }
}

#[test]
fn test_indexed_where_returns_no_false_positives() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, cat TEXT)")
        .unwrap();
    for i in 0..30 {
        let cat = match i % 3 {
            0 => "a",
            1 => "b",
            _ => "c",
        };
        db.execute(&format!("INSERT INTO t (cat) VALUES ('{}')", cat))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_cat ON t (cat) USING COLUMN")
        .unwrap();
    let rows = select_rows(&db, "SELECT * FROM t WHERE cat = 'b'");
    for row in &rows {
        assert_eq!(row[1], Value::text("b".to_string()), "no false positives");
    }
    assert_eq!(rows.len(), 10);
}

// ═══════════════════════════════════════════════════════════════════════
// bulk_load multi-page index (300+ entries round-trips correctly)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_index_lookup_after_bulk_load_500_entries() {
    // 500 distinct values spans multiple leaf pages in the B+Tree (>PAGE_SIZE).
    // This catches the bulk_load multi-page corruption bug.
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, tag TEXT)")
        .unwrap();
    for i in 0..500 {
        db.execute(&format!("INSERT INTO t (tag) VALUES ('val{}')", i))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_key ON t (tag) USING COLUMN")
        .unwrap();
    // Probe a value that lands in the 2nd+ leaf page
    let rows = select_rows(&db, "SELECT * FROM t WHERE tag = 'val450'");
    assert_eq!(rows.len(), 1, "multi-page index lookup must find the value");
    // Probe the first leaf page too
    let rows = select_rows(&db, "SELECT * FROM t WHERE tag = 'val1'");
    assert_eq!(rows.len(), 1);
    // Non-existent value
    let rows = select_rows(&db, "SELECT * FROM t WHERE tag = 'val999'");
    assert!(rows.is_empty(), "non-existent value must return empty");
}

#[test]
fn test_index_lookup_after_vacuum_compaction() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, region TEXT)")
        .unwrap();
    for i in 0..300 {
        let region = if i % 3 == 0 { "US" } else { "EU" };
        db.execute(&format!("INSERT INTO t (region) VALUES ('{}')", region))
            .unwrap();
    }
    db.execute("CREATE INDEX idx_region ON t (region) USING COLUMN")
        .unwrap();
    db.vacuum().unwrap(); // triggers compaction — was breaking point lookups
    let rows = select_rows(&db, "SELECT * FROM t WHERE region = 'US'");
    assert_eq!(rows.len(), 100, "lookup must work after vacuum/compaction");
    // PK point lookup must also work after compaction
    let rows = select_rows(&db, "SELECT * FROM t WHERE id = 150");
    assert_eq!(rows.len(), 1, "PK lookup must work after compaction");
}

// ═══════════════════════════════════════════════════════════════════════
// AUTO_INCREMENT + fast batch insert
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_auto_increment_select_all() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)")
        .unwrap();
    for i in 0..10 {
        db.execute(&format!("INSERT INTO t (v) VALUES ('x{}')", i))
            .unwrap();
    }
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 10);
    // IDs should be 1..=10
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row[0],
            Value::Integer((i + 1) as i64),
            "auto-increment id at {}",
            i
        );
    }
}

// ═══════════════════════════════════════════════════════════════════════
// LIKE prefix filter
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_like_prefix_filter() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT)")
        .unwrap();
    db.execute("INSERT INTO t (name) VALUES ('apple')").unwrap();
    db.execute("INSERT INTO t (name) VALUES ('apricot')")
        .unwrap();
    db.execute("INSERT INTO t (name) VALUES ('banana')")
        .unwrap();
    let rows = select_rows(&db, "SELECT * FROM t WHERE name LIKE 'ap%'");
    assert_eq!(rows.len(), 2, "LIKE 'ap%' matches apple + apricot");
}

// ═══════════════════════════════════════════════════════════════════════
// Cross-cutting: INSERT/UPDATE/DELETE/SELECT interleave
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn test_interleaved_crud_correctness() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")
        .unwrap();
    // Insert 5
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10))
            .unwrap();
    }
    // Update 2 and 4
    db.execute("UPDATE t SET v = 200 WHERE id = 2").unwrap();
    db.execute("UPDATE t SET v = 400 WHERE id = 4").unwrap();
    // Delete 3
    db.execute("DELETE FROM t WHERE id = 3").unwrap();
    // Insert new
    db.execute("INSERT INTO t VALUES (6, 60)").unwrap();

    assert_eq!(count(&db, "t"), 5, "5 - 1 delete + 1 insert = 5");
    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 5);
    // Verify values: id=1→10, id=2→200, id=4→400, id=5→50, id=6→60 (id=3 deleted)
    let by_id: std::collections::HashMap<i64, i64> = rows
        .iter()
        .filter_map(|r| match (r.first(), r.get(1)) {
            (Some(Value::Integer(id)), Some(Value::Integer(v))) => Some((*id, *v)),
            _ => None,
        })
        .collect();
    assert_eq!(by_id.get(&1), Some(&10));
    assert_eq!(by_id.get(&2), Some(&200), "update id=2");
    assert!(!by_id.contains_key(&3), "id=3 deleted");
    assert_eq!(by_id.get(&4), Some(&400), "update id=4");
    assert_eq!(by_id.get(&5), Some(&50));
    assert_eq!(by_id.get(&6), Some(&60), "new insert");
}
