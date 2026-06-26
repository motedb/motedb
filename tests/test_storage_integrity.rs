//! Storage-layer integrity tests: tombstone handling, compaction correctness,
//! newest-version-wins across flush/merge/restart.
//!
//! These tests target the v0.5.0 storage bugs and the high-risk areas flagged
//! by coverage analysis: dedup_keys_newest_wins tombstone flag preservation,
//! count_live_rows after DELETE + compaction, and merge_segments mixed-key
//! scenarios. They form the regression baseline for future storage work.

use motedb::{Database, DBConfig, QueryResult};
use motedb::types::Value;
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
        _ => panic!("expected Select"),
    }
}

fn count(db: &Database, table: &str) -> i64 {
    match select_rows(db, &format!("SELECT COUNT(*) FROM {}", table)).first() {
        Some(r) => match r.first() {
            Some(Value::Integer(n)) => *n,
            _ => panic!("COUNT returned non-integer"),
        },
        None => panic!("COUNT returned no rows"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Tombstone flag preservation through dedup_keys_newest_wins (the bug fix)
// ═══════════════════════════════════════════════════════════════════════════

/// INSERT then DELETE the same key in one flush window. The dedup must keep
/// the newest version (tombstone) WITH deleted=true, so the row stays gone.
#[test]
fn test_insert_then_delete_same_key_one_flush() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    // The INSERT + DELETE are both in the write buffer; a query triggers flush,
    // which runs dedup_keys_newest_wins. The row must NOT be resurrected.
    assert_eq!(count(&db, "t"), 0, "INSERT+DELETE in one flush must yield 0 rows");
    assert!(select_rows(&db, "SELECT * FROM t").is_empty());
}

/// Same scenario but survives a checkpoint + reopen. The tombstone must
/// persist to disk with deleted=true.
#[test]
fn test_insert_then_delete_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
        db.execute("INSERT INTO t VALUES (1, 100)").unwrap();
        db.execute("INSERT INTO t VALUES (2, 200)").unwrap();
        db.execute("DELETE FROM t WHERE id = 1").unwrap();
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(count(&db, "t"), 1, "id=1 deleted, id=2 remains");
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Value::Integer(2), "only id=2 should remain");
}

/// UPDATE then DELETE: the key goes through 3 versions (insert, update, delete)
/// within one flush. Newest (delete) must win.
#[test]
fn test_update_then_delete_one_flush() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("UPDATE t SET v = 20 WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    assert_eq!(count(&db, "t"), 0, "3 versions, newest=delete, count must be 0");
}

/// Multiple keys, some deleted, in one flush window. Only live keys survive.
#[test]
fn test_mixed_insert_delete_one_flush() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i * 10)).unwrap();
    }
    // Delete even-numbered rows
    for i in (2..=10).step_by(2) {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i)).unwrap();
    }
    assert_eq!(count(&db, "t"), 5, "5 odd rows survive, 5 even deleted");
    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    for row in &rows {
        let id = match row.first() { Some(Value::Integer(n)) => *n, _ => -1 };
        assert!(id % 2 == 1, "deleted even id={} should not appear", id);
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// count_live_rows after DELETE + compaction
// ═══════════════════════════════════════════════════════════════════════════

/// DELETE rows, then force compaction (vacuum), then COUNT. The count must
/// still reflect the deletions (was a known bug with an ignored test).
#[test]
fn test_count_live_rows_after_delete_and_compaction() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").unwrap();
    for i in 0..300 {
        db.execute(&format!("INSERT INTO t (v) VALUES ('val{}')", i)).unwrap();
    }
    // Delete every 3rd row
    for i in (0..300).step_by(3) {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i + 1)).unwrap();
    }
    db.vacuum().unwrap();
    assert_eq!(count(&db, "t"), 200, "300 - 100 deleted = 200 after compaction");
}

/// COUNT must agree with full SELECT after deletes + compaction (consistency).
#[test]
fn test_count_matches_select_after_delete_compaction() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v INT)").unwrap();
    for i in 0..150 {
        db.execute(&format!("INSERT INTO t (v) VALUES ({})", i)).unwrap();
    }
    db.execute("DELETE FROM t WHERE id < 50").unwrap();
    db.vacuum().unwrap();
    let c = count(&db, "t");
    let rows = select_rows(&db, "SELECT * FROM t");
    assert_eq!(c as usize, rows.len(), "COUNT must match SELECT row count");
    assert_eq!(c, 101, "ids 50..150 remain (101 rows)");
}

// ═══════════════════════════════════════════════════════════════════════════
// merge_segments mixed scenarios (multi-key + multi-version + tombstone)
// ═══════════════════════════════════════════════════════════════════════════

/// Two flush batches create two segments. Some keys updated across the
/// boundary. Compaction must keep the newest version per key.
#[test]
fn test_compaction_multi_segment_newest_wins() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    // Segment 1: keys 1-5
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }
    db.checkpoint().unwrap(); // flush segment 1
    // Segment 2: update keys 2,4 + insert key 6
    db.execute("UPDATE t SET v = 200 WHERE id = 2").unwrap();
    db.execute("UPDATE t SET v = 400 WHERE id = 4").unwrap();
    db.execute("INSERT INTO t VALUES (6, 60)").unwrap();
    db.checkpoint().unwrap(); // flush segment 2
    db.vacuum().unwrap(); // compact → single segment
    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    let by_id: std::collections::HashMap<i64, i64> = rows.iter().filter_map(|r| {
        match (r.first(), r.get(1)) {
            (Some(Value::Integer(id)), Some(Value::Integer(v))) => Some((*id, *v)),
            _ => None,
        }
    }).collect();
    assert_eq!(by_id.len(), 6);
    assert_eq!(by_id.get(&2), Some(&200), "compaction keeps newest (updated) value");
    assert_eq!(by_id.get(&4), Some(&400), "compaction keeps newest (updated) value");
    assert_eq!(by_id.get(&6), Some(&60), "new key from segment 2");
    assert_eq!(by_id.get(&1), Some(&1), "unchanged key from segment 1");
}

/// Delete a key in segment 2 that exists in segment 1. Compaction must
/// propagate the tombstone (key removed from merged result).
#[test]
fn test_compaction_cross_segment_delete() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
    }
    db.checkpoint().unwrap();
    db.execute("DELETE FROM t WHERE id = 3").unwrap();
    db.checkpoint().unwrap();
    db.vacuum().unwrap();
    assert_eq!(count(&db, "t"), 4, "id=3 deleted across segments");
    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    for row in &rows {
        let id = match row.first() { Some(Value::Integer(n)) => *n, _ => -1 };
        assert_ne!(id, 3, "deleted key must not appear after compaction");
    }
}

/// Three segments, interleaved updates and deletes. Full stress of the merge.
#[test]
fn test_compaction_three_segments_mixed() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    // Seg 1: 1-10
    for i in 1..=10 { db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap(); }
    db.checkpoint().unwrap();
    // Seg 2: update 5, delete 1,2
    db.execute("UPDATE t SET v = 555 WHERE id = 5").unwrap();
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.checkpoint().unwrap();
    // Seg 3: update 5 again, delete 10, insert 11
    db.execute("UPDATE t SET v = 999 WHERE id = 5").unwrap();
    db.execute("DELETE FROM t WHERE id = 10").unwrap();
    db.execute("INSERT INTO t VALUES (11, 110)").unwrap();
    db.checkpoint().unwrap();
    db.vacuum().unwrap();

    assert_eq!(count(&db, "t"), 8, "10 - 3 deleted + 1 inserted = 8");
    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    let by_id: std::collections::HashMap<i64, i64> = rows.iter().filter_map(|r| {
        match (r.first(), r.get(1)) {
            (Some(Value::Integer(id)), Some(Value::Integer(v))) => Some((*id, *v)),
            _ => None,
        }
    }).collect();
    assert_eq!(by_id.get(&5), Some(&999), "double-updated key keeps final value");
    assert!(!by_id.contains_key(&1) && !by_id.contains_key(&2) && !by_id.contains_key(&10));
    assert_eq!(by_id.get(&11), Some(&110));
}

// ═══════════════════════════════════════════════════════════════════════════
// get_table_row (PK point lookup) after compaction
// ═══════════════════════════════════════════════════════════════════════════

/// After compaction, every surviving PK must be findable via point lookup.
/// This validates find_key() binary search over the merged segment.
#[test]
fn test_pk_point_lookup_after_compaction() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, v TEXT)").unwrap();
    for i in 0..500 {
        db.execute(&format!("INSERT INTO t (v) VALUES ('v{}')", i)).unwrap();
    }
    db.vacuum().unwrap();
    // Point-lookup several IDs across the key space
    for target_id in [1, 100, 250, 499] {
        let rows = select_rows(&db, &format!("SELECT * FROM t WHERE id = {}", target_id));
        assert_eq!(rows.len(), 1, "PK {} must be found after compaction", target_id);
        let expected = format!("v{}", target_id - 1);
        assert_eq!(rows[0][1], Value::text(expected));
    }
    // Non-existent
    let rows = select_rows(&db, "SELECT * FROM t WHERE id = 99999");
    assert!(rows.is_empty());
}

/// After UPDATE + compaction, PK lookup returns the NEW value.
#[test]
fn test_pk_lookup_returns_newest_after_update_compaction() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    db.execute("INSERT INTO t VALUES (7, 100)").unwrap();
    db.checkpoint().unwrap();
    db.execute("UPDATE t SET v = 777 WHERE id = 7").unwrap();
    db.checkpoint().unwrap();
    db.vacuum().unwrap();
    let rows = select_rows(&db, "SELECT * FROM t WHERE id = 7");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], Value::Integer(777), "PK lookup must return updated value");
}

// ═══════════════════════════════════════════════════════════════════════════
// Repeated flush boundaries (many small checkpoints)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_many_checkpoints_then_compact() {
    let (_dir, db) = make_db();
    db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").unwrap();
    // Insert + checkpoint in a loop, creating many segments
    for i in 1..=20 {
        db.execute(&format!("INSERT INTO t VALUES ({}, {})", i, i)).unwrap();
        db.checkpoint().unwrap();
    }
    // Update half of them
    for i in 1..=10 {
        db.execute(&format!("UPDATE t SET v = {} WHERE id = {}", i * 100, i)).unwrap();
        db.checkpoint().unwrap();
    }
    db.vacuum().unwrap();
    assert_eq!(count(&db, "t"), 20);
    let rows = select_rows(&db, "SELECT * FROM t ORDER BY id");
    assert_eq!(rows.len(), 20);
    // Updated ids have value = id * 100
    let by_id: std::collections::HashMap<i64, i64> = rows.iter().filter_map(|r| {
        match (r.first(), r.get(1)) {
            (Some(Value::Integer(id)), Some(Value::Integer(v))) => Some((*id, *v)),
            _ => None,
        }
    }).collect();
    assert_eq!(by_id.get(&5), Some(&500));
    assert_eq!(by_id.get(&15), Some(&15), "id 11-20 unchanged");
}
