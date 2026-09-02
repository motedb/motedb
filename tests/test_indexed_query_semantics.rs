//! Regression tests for indexed-equality query semantics.
//!
//! Covers three bug classes found by the scaling audit:
//! 1. Bulk multi-row INSERT (≥100 rows, fast_batch_insert) never updated
//!    secondary column indexes → `COUNT(*) WHERE col = v` returned 0.
//! 2. `WHERE indexed = ? LIMIT k` did not push LIMIT below the row fetch
//!    (latency grew linearly with table size), and one code path dropped
//!    LIMIT entirely (returned more than k rows).
//! 3. Column index keys truncate Text values to a 64-byte prefix — an
//!    equality query must not return a row whose value merely shares the
//!    prefix (index false positive).

use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, cat TEXT, val INT)")
        .unwrap();
    db.execute("CREATE INDEX idx_cat ON t (cat) USING COLUMN")
        .unwrap();
    (db, dir)
}

fn bulk_insert(db: &Database, n: usize, start: usize) {
    // One multi-row INSERT of ≥100 rows → routes through fast_batch_insert.
    let mut batch = String::new();
    for i in start..start + n {
        batch.push_str(&format!("('cat_{}', {}),", i % 10, i));
    }
    batch.truncate(batch.len() - 1);
    db.execute(&format!("INSERT INTO t (cat, val) VALUES {}", batch))
        .unwrap();
}

fn count_cat(db: &Database, cat: &str) -> i64 {
    let r = db
        .execute(&format!("SELECT COUNT(*) FROM t WHERE cat = '{}'", cat))
        .unwrap()
        .materialize()
        .unwrap();
    if let Some((_, rows)) = r.select_rows() {
        if let Some(motedb::types::Value::Integer(n)) =
            rows.first().and_then(|r| r.first().cloned())
        {
            return n;
        }
    }
    panic!("no count returned");
}

#[test]
fn test_bulk_insert_updates_column_index_count() {
    // BUG: fast_batch_insert (batches ≥100 rows on AUTO_INCREMENT tables)
    // skipped column index maintenance entirely — COUNT(*) via the index
    // returned 0 for values with hundreds of matches.
    let (db, _dir) = db();
    bulk_insert(&db, 500, 0); // 50 rows per cat_0..cat_9
    assert_eq!(count_cat(&db, "cat_3"), 50, "index must reflect bulk rows");
    assert_eq!(count_cat(&db, "cat_0"), 50);

    // A second bulk batch must also be reflected.
    bulk_insert(&db, 500, 0);
    assert_eq!(count_cat(&db, "cat_3"), 100);
    assert_eq!(count_cat(&db, "cat_9"), 100);
}

#[test]
fn test_indexed_equality_limit_returns_exactly_k() {
    let (db, _dir) = db();
    bulk_insert(&db, 300, 0); // 30 rows per cat

    for k in [1usize, 5, 29, 30, 31, 300] {
        let r = db
            .execute(&format!("SELECT id FROM t WHERE cat = 'cat_1' LIMIT {}", k))
            .unwrap()
            .materialize()
            .unwrap();
        let n = r.select_rows().map(|(_, rows)| rows.len()).unwrap_or(0);
        assert_eq!(n, k.min(30), "LIMIT {} must return {} rows", k, k.min(30));
    }
}

#[test]
fn test_indexed_equality_limit_prefix_matches_no_limit() {
    // LIMIT must return a PREFIX of the unlimited result (same order), not
    // an arbitrary different subset.
    let (db, _dir) = db();
    bulk_insert(&db, 200, 0);

    let full = db
        .execute("SELECT id FROM t WHERE cat = 'cat_2'")
        .unwrap()
        .materialize()
        .unwrap();
    let limited = db
        .execute("SELECT id FROM t WHERE cat = 'cat_2' LIMIT 7")
        .unwrap()
        .materialize()
        .unwrap();
    let full_rows = full
        .select_rows()
        .map(|(_, r)| r.to_vec())
        .unwrap_or_default();
    let lim_rows = limited
        .select_rows()
        .map(|(_, r)| r.to_vec())
        .unwrap_or_default();
    assert_eq!(full_rows.len(), 20);
    assert_eq!(lim_rows.len(), 7);
    for (i, lr) in lim_rows.iter().enumerate() {
        assert_eq!(lr, &full_rows[i], "LIMIT result must be a prefix");
    }
}

#[test]
fn test_indexed_equality_offset_limit() {
    let (db, _dir) = db();
    bulk_insert(&db, 200, 0); // 20 per cat

    let full = db
        .execute("SELECT id FROM t WHERE cat = 'cat_4'")
        .unwrap()
        .materialize()
        .unwrap();
    let paged = db
        .execute("SELECT id FROM t WHERE cat = 'cat_4' LIMIT 5 OFFSET 10")
        .unwrap()
        .materialize()
        .unwrap();
    let full_rows = full
        .select_rows()
        .map(|(_, r)| r.to_vec())
        .unwrap_or_default();
    let paged_rows = paged
        .select_rows()
        .map(|(_, r)| r.to_vec())
        .unwrap_or_default();
    assert_eq!(paged_rows.len(), 5);
    for (i, pr) in paged_rows.iter().enumerate() {
        assert_eq!(pr, &full_rows[10 + i], "OFFSET+LIMIT window mismatch");
    }

    // OFFSET beyond result set → empty.
    let empty = db
        .execute("SELECT id FROM t WHERE cat = 'cat_4' LIMIT 5 OFFSET 20")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(empty.select_rows().map(|(_, r)| r.len()).unwrap_or(0), 0);
}

#[test]
fn test_indexed_equality_no_match_returns_zero() {
    let (db, _dir) = db();
    bulk_insert(&db, 150, 0);
    assert_eq!(count_cat(&db, "cat_3"), 15);
    // Genuinely-missing value: COUNT must be 0 (not error, not stale count).
    assert_eq!(count_cat(&db, "no_such_cat"), 0);
    let r = db
        .execute("SELECT id FROM t WHERE cat = 'no_such_cat'")
        .unwrap()
        .materialize()
        .unwrap();
    assert_eq!(r.select_rows().map(|(_, r)| r.len()).unwrap_or(0), 0);
}

#[test]
fn test_indexed_in_list_no_stale_empty() {
    // IN-list path: when NO value of the list is found in the (possibly
    // stale) index, the executor must fall back to a scan rather than
    // authoritatively returning empty.
    let (db, _dir) = db();
    bulk_insert(&db, 150, 0);
    let r = db
        .execute("SELECT COUNT(*) FROM t WHERE cat IN ('cat_1', 'cat_2')")
        .unwrap()
        .materialize()
        .unwrap();
    if let Some((_, rows)) = r.select_rows() {
        if let Some(motedb::types::Value::Integer(n)) =
            rows.first().and_then(|r| r.first().cloned())
        {
            assert_eq!(n, 30, "IN-list over indexed column");
            return;
        }
    }
    panic!("no count returned");
}

#[test]
fn test_text_prefix_no_false_positive() {
    // Column index keys truncate Text values to a 64-byte prefix. Two
    // distinct values sharing that prefix must not cross-match: querying
    // for one must not return rows of the other.
    let (db, _dir) = db();
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let _ = (dir, config);

    let prefix = "P".repeat(80); // > 64 bytes
    let a = format!("{}-alpha", prefix);
    let b = format!("{}-beta", prefix);

    // Insert via single rows (per-row path) so both land in the index.
    db.execute(&format!("INSERT INTO t (cat, val) VALUES ('{}', 1)", a))
        .unwrap();
    db.execute(&format!("INSERT INTO t (cat, val) VALUES ('{}', 2)", b))
        .unwrap();
    // And via a bulk batch of the same long values.
    let mut batch = String::new();
    for i in 0..120 {
        let v = if i % 2 == 0 { &a } else { &b };
        batch.push_str(&format!("('{}', {}),", v, i));
    }
    batch.truncate(batch.len() - 1);
    db.execute(&format!("INSERT INTO t (cat, val) VALUES {}", batch))
        .unwrap();

    let count_a = count_cat(&db, &a);
    assert_eq!(count_a, 61, "alpha rows: 1 single + 60 bulk");
    let count_b = count_cat(&db, &b);
    assert_eq!(count_b, 61, "beta rows: 1 single + 60 bulk");

    // Exact-match fetch must also return only the requested value's rows.
    let r = db
        .execute(&format!("SELECT val FROM t WHERE cat = '{}' LIMIT 3", a))
        .unwrap()
        .materialize()
        .unwrap();
    if let Some((_, rows)) = r.select_rows() {
        assert_eq!(rows.len(), 3);
    } else {
        panic!("no rows");
    }
}

#[test]
fn test_update_moves_bulk_inserted_index_entries() {
    // UPDATE that changes the indexed column must move the index entry:
    // rows inserted via bulk batch (fast_batch_insert) must behave the same
    // as single-row inserts.
    let (db, _dir) = db();
    bulk_insert(&db, 300, 0); // 30 per cat_0..cat_9
    assert_eq!(count_cat(&db, "cat_1"), 30);
    assert_eq!(count_cat(&db, "cat_8"), 30);

    // Ground truth BEFORE: materialize all cat_1 rows (scan, index-free).
    let scan_count = |cat: &str| -> usize {
        db.execute(&format!(
            "SELECT id FROM t WHERE cat = '{}' AND val >= 0",
            cat
        ))
        .unwrap()
        .materialize()
        .unwrap()
        .select_rows()
        .map(|(_, r)| r.len())
        .unwrap_or(0)
    };
    let c1_before = scan_count("cat_1");
    let c8_before = scan_count("cat_8");

    let upd = db
        .execute("UPDATE t SET cat = 'cat_8' WHERE cat = 'cat_1' AND id <= 10")
        .unwrap();
    let affected = upd.affected_rows();

    // Scan-based ground truth after the move.
    let c1_after = scan_count("cat_1");
    let c8_after = scan_count("cat_8");
    assert_eq!(
        c1_before - c1_after,
        affected,
        "source lost exactly affected rows (scan)"
    );
    assert_eq!(
        c8_after - c8_before,
        affected,
        "destination gained exactly affected rows (scan)"
    );
    assert!(affected > 0);

    // Index counts must agree with scan counts.
    assert_eq!(
        count_cat(&db, "cat_1"),
        c1_after as i64,
        "index count == scan count (source)"
    );
    assert_eq!(
        count_cat(&db, "cat_8"),
        c8_after as i64,
        "index count == scan count (dest)"
    );
}

#[test]
fn test_delete_bulk_inserted_rows_updates_index() {
    let (db, _dir) = db();
    bulk_insert(&db, 300, 0);
    assert_eq!(count_cat(&db, "cat_2"), 30);

    db.execute("DELETE FROM t WHERE cat = 'cat_2' AND id <= 100")
        .unwrap();
    let before = count_cat(&db, "cat_2");
    assert!(before < 30, "DELETE must remove rows ({})", before);

    // Remaining cat_2 rows must still be fetchable by index and match scan.
    let by_scan = db
        .execute("SELECT COUNT(*) FROM t WHERE cat = 'cat_2'")
        .unwrap()
        .materialize()
        .unwrap();
    let _ = by_scan;
    assert_eq!(count_cat(&db, "cat_2"), before);

    // Rows of other cats untouched.
    assert_eq!(count_cat(&db, "cat_3"), 30);
}

#[test]
fn test_index_rebuilt_correctly_after_reopen() {
    // Reopen path: column indexes are derived data, rebuilt from storage on
    // open. Bulk-inserted rows must be findable after reopen.
    let dir = TempDir::new().unwrap();
    {
        let mut config = DBConfig::for_testing();
        config.auto_checkpoint = None;
        let db = Database::create_with_config(dir.path(), config).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, cat TEXT, val INT)")
            .unwrap();
        db.execute("CREATE INDEX idx_cat ON t (cat) USING COLUMN")
            .unwrap();
        bulk_insert(&db, 250, 0);
        db.execute("CHECKPOINT").unwrap();
        assert_eq!(
            db.execute("SELECT COUNT(*) FROM t WHERE cat = 'cat_5'")
                .unwrap()
                .materialize()
                .unwrap()
                .select_rows()
                .map(|(_, r)| r.len())
                .unwrap_or(0),
            1
        );
    }
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db2 = Database::open_with_config(dir.path(), config).unwrap();
    let r = db2
        .execute("SELECT COUNT(*) FROM t WHERE cat = 'cat_5'")
        .unwrap()
        .materialize()
        .unwrap();
    if let Some((_, rows)) = r.select_rows() {
        if let Some(motedb::types::Value::Integer(n)) =
            rows.first().and_then(|r| r.first().cloned())
        {
            assert_eq!(n, 25, "index must serve correct count after reopen");
            return;
        }
    }
    panic!("no count returned");
}
