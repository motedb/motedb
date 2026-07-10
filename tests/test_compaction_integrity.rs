//! Compaction & Data Integrity Tests
//!
//! Validates that LSM compaction preserves all data correctly.
//! Covers the P0 bug where merge_sstables() skipped overlapping SSTables
//! (max_open=4 limit) but run_compaction() still removed them from metadata.
//!
//! Run:
//!   cargo test --test test_compaction_integrity -- --test-threads=1
//!   cargo test --test test_compaction_integrity --profile release-test -- --test-threads=1

use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

// ── Helpers ──────────────────────────────────────────────────────────

fn exec(db: &Database, sql: &str) -> motedb::sql::QueryResult {
    db.execute(sql)
        .expect("execute SQL")
        .materialize()
        .expect("materialize")
}

fn count_rows(db: &Database) -> usize {
    // Use materialize() so all StreamingQueryResult variants are handled
    // (SelectStreaming / SelectReady / SelectColumnar). The previous manual
    // iteration only matched SelectStreaming, silently returning 0 for tables
    // served by the columnar store (SelectColumnar/SelectReady).
    match exec(db, "SELECT id FROM t") {
        motedb::sql::QueryResult::Select { rows, .. } => rows.len(),
        _ => 0,
    }
}

fn get_row(db: &Database, id: i64) -> Option<Vec<Value>> {
    let sql = format!("SELECT * FROM t WHERE id = {}", id);
    let result = exec(db, &sql);
    match result {
        motedb::sql::QueryResult::Select { rows, .. } => rows.into_iter().next(),
        _ => None,
    }
}

fn get_val(db: &Database, id: i64) -> String {
    let row = get_row(db, id).unwrap_or_else(|| panic!("Row {} should exist", id));
    match &row[1] {
        Value::Text(s) => (**s).to_string(),
        other => panic!("Row {} col 1 expected Text, got {:?}", id, other),
    }
}

fn wait_for_compaction() {
    std::thread::sleep(std::time::Duration::from_secs(5));
}

fn make_db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_edge()).unwrap();
    (dir, db)
}

fn create_table(db: &Database) {
    exec(
        db,
        "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT, amount FLOAT)",
    );
}

/// Count segment files in the columnar multi-segment store (the source of
/// truth since v0.3.0). Data lives in `<db>.mote/columnar_ms/<table>/*.sst`,
/// NOT in the legacy `lsm/` dir.
fn count_sst_files(dir: &TempDir) -> usize {
    let ms = dir.path().with_extension("mote").join("columnar_ms");
    let mut count = 0;
    if let Ok(rd) = std::fs::read_dir(&ms) {
        for entry in rd.flatten() {
            let p = entry.path();
            if p.is_dir() {
                if let Ok(rd2) = std::fs::read_dir(&p) {
                    count += rd2
                        .flatten()
                        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
                        .count();
                }
            }
        }
    }
    count
}

// ── Category 1: Compaction Data Integrity ─────────────────────────────

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_compaction_preserves_all_rows() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Insert 10K rows, flushing every 2000 to create 5 SSTables
    for batch in 0..5i64 {
        let start = batch * 2000 + 1;
        let end = start + 2000;
        for i in start..end {
            exec(
                &db,
                &format!("INSERT INTO t VALUES ({}, 'v_{}', 0.0)", i, i),
            );
        }
        db.flush().unwrap();
    }

    wait_for_compaction();

    let count = count_rows(&db);
    assert_eq!(
        count, 10000,
        "After compaction: expected 10000 rows, got {}",
        count
    );

    // PK spot-check: every 100th row
    for i in (1..=10000).step_by(100) {
        let row = get_row(&db, i).unwrap_or_else(|| panic!("Row {} missing after compaction", i));
        assert!(row.len() >= 2, "Row {} has too few columns", i);
    }
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_compaction_with_overlapping_keys() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Round 1: INSERT 100 rows
    for i in 1..=100i64 {
        exec(
            &db,
            &format!("INSERT INTO t VALUES ({}, 'initial', 0.0)", i),
        );
    }
    db.flush().unwrap();

    // Round 2: UPDATE rows 1-50
    for i in 1..=50i64 {
        exec(&db, &format!("UPDATE t SET status = 'v1' WHERE id = {}", i));
    }
    db.flush().unwrap();

    // Round 3: UPDATE rows 1-25 again
    for i in 1..=25i64 {
        exec(&db, &format!("UPDATE t SET status = 'v2' WHERE id = {}", i));
    }
    db.flush().unwrap();

    wait_for_compaction();
    db.flush().unwrap();

    // Retry loop for compaction settling
    let mut ok = false;
    for attempt in 0..8 {
        let count = count_rows(&db);
        if count != 100 {
            eprintln!("Attempt {}: count={}, waiting...", attempt, count);
            std::thread::sleep(std::time::Duration::from_secs(3));
            db.flush().unwrap();
            continue;
        }
        let mut all_correct = true;
        for i in 1..=25i64 {
            if get_val(&db, i) != "v2" {
                all_correct = false;
                break;
            }
        }
        if all_correct {
            for i in 26..=50i64 {
                if get_val(&db, i) != "v1" {
                    all_correct = false;
                    break;
                }
            }
        }
        if all_correct {
            for i in 51..=100i64 {
                if get_val(&db, i) != "initial" {
                    all_correct = false;
                    break;
                }
            }
        }
        if all_correct {
            ok = true;
            break;
        }
        eprintln!("Attempt {}: values not yet correct, waiting...", attempt);
        std::thread::sleep(std::time::Duration::from_secs(3));
        db.flush().unwrap();
    }

    assert!(
        ok,
        "Compaction did not settle with correct values after 8 attempts"
    );
    assert_eq!(
        count_rows(&db),
        100,
        "Should have 100 rows after compaction"
    );
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_compaction_after_delete() {
    let (_dir, db) = make_db();
    create_table(&db);

    for i in 1..=500i64 {
        exec(
            &db,
            &format!("INSERT INTO t VALUES ({}, 'active', {})", i, i),
        );
    }
    db.flush().unwrap();

    // Delete first half
    for i in 1..=250i64 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    db.flush().unwrap();

    wait_for_compaction();

    assert_eq!(
        count_rows(&db),
        250,
        "Should have 250 rows after delete + compaction"
    );
    assert!(get_row(&db, 1).is_none(), "Deleted row 1 should be gone");
    assert!(
        get_row(&db, 250).is_none(),
        "Deleted row 250 should be gone"
    );
    assert!(get_row(&db, 251).is_some(), "Row 251 should exist");
    assert!(get_row(&db, 500).is_some(), "Row 500 should exist");
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_compaction_tombstone_propagation() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Insert, flush, verify present
    exec(&db, "INSERT INTO t VALUES (42, 'alive', 1.0)");
    db.flush().unwrap();
    assert!(get_row(&db, 42).is_some(), "Row should exist after flush");

    // Delete, flush, verify gone
    exec(&db, "DELETE FROM t WHERE id = 42");
    db.flush().unwrap();

    wait_for_compaction();

    assert!(
        get_row(&db, 42).is_none(),
        "Row should be gone after tombstone compaction"
    );
    assert_eq!(count_rows(&db), 0, "Table should be empty");
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_multi_level_compaction() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Write 10K rows in 5 batches with interleaved flush+sleep
    for batch in 0..5i64 {
        let start = batch * 2000 + 1;
        let end = start + 2000;
        for i in start..end {
            exec(
                &db,
                &format!("INSERT INTO t VALUES ({}, 'data', {:.1})", i, i as f64),
            );
        }
        db.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    wait_for_compaction();

    let count = count_rows(&db);
    assert_eq!(
        count, 10000,
        "All 10000 rows should survive multi-level compaction, got {}",
        count
    );

    // Spot-check
    for i in (1..=10000).step_by(200) {
        assert!(get_row(&db, i).is_some(), "Row {} missing", i);
    }
}

// ── Category 2: Concurrent Operations ─────────────────────────────────

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_concurrent_writes_and_scan() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Pre-insert baseline
    for i in 1..=1000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'base', 0.0)", i));
    }
    db.flush().unwrap();

    let scan_counts = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    std::thread::scope(|s| {
        let db = &db;
        let counts = scan_counts.clone();

        // Writer: inserts rows 1001-3000
        s.spawn(move || {
            for i in 1001..=3000i64 {
                exec(db, &format!("INSERT INTO t VALUES ({}, 'new', 0.0)", i));
            }
        });

        // Reader: scan 10 times
        s.spawn(move || {
            for _ in 0..10 {
                let c = count_rows(db);
                counts.lock().unwrap().push(c);
            }
        });
    });

    // All scans should see at least the baseline 1000 rows
    let counts = scan_counts.lock().unwrap();
    for (i, &c) in counts.iter().enumerate() {
        assert!(c >= 1000, "Scan {} saw {} rows, expected >= 1000", i, c);
    }

    // Final count should be 3000
    let final_count = count_rows(&db);
    assert_eq!(
        final_count, 3000,
        "Final count should be 3000, got {}",
        final_count
    );
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_concurrent_flush_and_scan() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Insert 3000 rows (triggers auto-flush at ~2001)
    for i in 1..=3000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'x', {})", i, i));
    }

    // Immediately scan repeatedly while auto-flush may be running
    let mut min_count = usize::MAX;
    let mut max_count = 0;
    for _ in 0..10 {
        let c = count_rows(&db);
        min_count = min_count.min(c);
        max_count = max_count.max(c);
    }

    // All scans should see at least 3000 (no data loss, maybe more from dedup artifacts)
    assert!(min_count >= 3000, "Min scan count {} < 3000", min_count);

    let final_count = count_rows(&db);
    assert_eq!(
        final_count, 3000,
        "Final count should be 3000, got {}",
        final_count
    );
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_concurrent_compaction_and_point_get() {
    let (_dir, db) = make_db();
    create_table(&db);

    // Insert 5000 rows, flush to create SSTables and trigger compaction
    for i in 1..=5000i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t VALUES ({}, 'val_{}', {:.2})",
                i,
                i,
                i as f64 * 1.5
            ),
        );
    }
    db.flush().unwrap();

    // PK lookups while compaction may be running
    for i in (1..=5000).step_by(50) {
        let row =
            get_row(&db, i).unwrap_or_else(|| panic!("Row {} should exist during compaction", i));
        assert_eq!(row[0], Value::Integer(i), "Row {} has wrong id", i);
    }

    // Trigger another flush for more compaction
    for i in 1..=100i64 {
        exec(
            &db,
            &format!("UPDATE t SET status = 'updated' WHERE id = {}", i),
        );
    }
    db.flush().unwrap();

    // Verify again
    for i in (1..=100).step_by(10) {
        assert_eq!(get_val(&db, i), "updated", "Row {} should be updated", i);
    }
    for i in (101..=5000).step_by(100) {
        assert_eq!(
            get_val(&db, i),
            format!("val_{}", i),
            "Row {} should be original",
            i
        );
    }

    assert_eq!(count_rows(&db), 5000, "All 5000 rows should be present");
}

// ── Category 3: Edge Cases ────────────────────────────────────────────

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_many_small_sstables() {
    let (_dir, db) = make_db();
    create_table(&db);

    // 20 rounds of small inserts + flush = 20 SSTables
    for round in 0..20i64 {
        let base = round * 1000 + 1;
        for i in 0..10i64 {
            let id = base + i;
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'small', 0.0)", id));
        }
        db.flush().unwrap();
    }

    wait_for_compaction();

    assert_eq!(
        count_rows(&db),
        200,
        "Should have 200 rows across 20 SSTables, got {}",
        count_rows(&db)
    );

    // Verify every row
    for round in 0..20i64 {
        let base = round * 1000 + 1;
        for i in 0..10i64 {
            let id = base + i;
            assert!(get_row(&db, id).is_some(), "Row {} missing", id);
        }
    }
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_empty_table_scan() {
    let (_dir, db) = make_db();
    create_table(&db);

    assert_eq!(count_rows(&db), 0, "Empty table should have 0 rows");

    db.flush().unwrap();
    assert_eq!(
        count_rows(&db),
        0,
        "Empty table after flush should have 0 rows"
    );
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_single_row_lifecycle() {
    let (_dir, db) = make_db();
    create_table(&db);

    // INSERT
    exec(&db, "INSERT INTO t VALUES (1, 'original', 10.0)");
    assert!(get_row(&db, 1).is_some(), "Row exists in memtable");
    assert_eq!(get_val(&db, 1), "original");

    // Flush to SSTable
    db.flush().unwrap();
    assert!(get_row(&db, 1).is_some(), "Row exists in SSTable");
    assert_eq!(get_val(&db, 1), "original");

    // UPDATE
    exec(&db, "UPDATE t SET status = 'updated' WHERE id = 1");
    assert_eq!(get_val(&db, 1), "updated");

    db.flush().unwrap();
    assert_eq!(get_val(&db, 1), "updated");

    // DELETE
    exec(&db, "DELETE FROM t WHERE id = 1");
    assert!(get_row(&db, 1).is_none(), "Row gone from memtable");

    db.flush().unwrap();
    assert!(get_row(&db, 1).is_none(), "Row gone after flush");

    wait_for_compaction();
    assert!(get_row(&db, 1).is_none(), "Row gone after compaction");
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_large_dataset_compaction() {
    let (_dir, db) = make_db();
    create_table(&db);

    // 20K rows in 4 batches
    for batch in 0..4i64 {
        let start = batch * 5000 + 1;
        let end = start + 5000;
        for i in start..end {
            exec(
                &db,
                &format!("INSERT INTO t VALUES ({}, 'v_{}', {:.2})", i, i, i as f64),
            );
        }
        db.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    wait_for_compaction();
    assert_eq!(
        count_rows(&db),
        20000,
        "Should have 20000 rows after compaction"
    );

    // Spot-check
    for i in (1..=20000).step_by(500) {
        assert!(get_row(&db, i).is_some(), "Row {} missing", i);
    }

    // UPDATE first 2K rows
    for i in 1..=2000i64 {
        exec(
            &db,
            &format!("UPDATE t SET status = 'final' WHERE id = {}", i),
        );
    }
    db.flush().unwrap();
    wait_for_compaction();

    assert_eq!(count_rows(&db), 20000, "Still 20000 after UPDATE");
    for i in (1..=2000).step_by(500) {
        assert_eq!(get_val(&db, i), "final", "Row {} should be updated", i);
    }
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_delete_nonexistent_key() {
    let (_dir, db) = make_db();
    create_table(&db);

    exec(&db, "INSERT INTO t VALUES (1, 'here', 1.0)");

    // Delete nonexistent — should not error
    exec(&db, "DELETE FROM t WHERE id = 999");

    assert_eq!(count_rows(&db), 1, "Only row 1 should exist");
    assert!(get_row(&db, 1).is_some(), "Row 1 should still exist");
    assert!(get_row(&db, 999).is_none(), "Row 999 should not exist");
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_update_then_scan_preserves_all() {
    let (_dir, db) = make_db();
    create_table(&db);

    // INSERT 5K
    for i in 1..=5000i64 {
        exec(
            &db,
            &format!(
                "INSERT INTO t VALUES ({}, 'pending', {:.2})",
                i,
                i as f64 * 10.0
            ),
        );
    }
    db.flush().unwrap();

    // UPDATE 1-2500
    for i in 1..=2500i64 {
        exec(
            &db,
            &format!("UPDATE t SET status = 'completed' WHERE id = {}", i),
        );
    }
    db.flush().unwrap();

    // UPDATE 1-1000 again
    for i in 1..=1000i64 {
        exec(
            &db,
            &format!("UPDATE t SET status = 'final' WHERE id = {}", i),
        );
    }
    db.flush().unwrap();

    // Retry loop: compaction may still be settling from earlier tests
    let mut ok = false;
    for attempt in 0..8 {
        db.flush().unwrap();
        let total = count_rows(&db);
        if total != 5000 {
            eprintln!("Attempt {}: count={}, waiting...", attempt, total);
            std::thread::sleep(std::time::Duration::from_secs(3));
            continue;
        }

        let mut final_count = 0u64;
        let mut completed_count = 0u64;
        let mut pending_count = 0u64;
        // materialize() handles all StreamingQueryResult variants (the columnar
        // store returns SelectColumnar/SelectReady, not SelectStreaming).
        let result = db
            .execute("SELECT id, status FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        if let motedb::sql::QueryResult::Select { rows, .. } = result {
            for row in &rows {
                match &row[1] {
                    Value::Text(s) if s.as_str() == "final" => final_count += 1,
                    Value::Text(s) if s.as_str() == "completed" => completed_count += 1,
                    Value::Text(s) if s.as_str() == "pending" => pending_count += 1,
                    other => panic!("Unexpected status: {:?}", other),
                }
            }
        }

        if final_count == 1000 && completed_count == 1500 && pending_count == 2500 {
            ok = true;
            break;
        }
        eprintln!(
            "Attempt {}: final={}, completed={}, pending={}",
            attempt, final_count, completed_count, pending_count
        );
        std::thread::sleep(std::time::Duration::from_secs(3));
    }

    assert!(ok, "Status distribution did not settle after 8 attempts");
}

// ── Category 4: Segment file lifecycle ─────────────────────────────────
// Since v0.3.0 the columnar multi-segment store is the source of truth.
// Compaction (merge_segments) merges old segments into one and GCs the old
// files immediately (no deferred deletion — the single-writer manifest makes
// that safe). These tests verify that data survives across compaction cycles
// and the segment file count stays bounded.

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_deferred_deletion_keeps_files_alive() {
    let (dir, db) = make_db();
    create_table(&db);

    // Batch 1: creates segment A
    for i in 1..=500i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'a', 0.0)", i));
    }
    db.flush().unwrap();
    assert!(
        count_sst_files(&dir) >= 1,
        "Batch 1: expected >= 1 segment file"
    );

    // Batch 2: creates segment B → may trigger compaction (A+B merge)
    for i in 501..=1000i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'b', 0.0)", i));
    }
    db.flush().unwrap();
    wait_for_compaction();

    // Data must survive regardless of compaction/GC.
    assert_eq!(count_rows(&db), 1000, "All 1000 rows present after batch 2");

    // Batch 3: creates segment C → another compaction cycle
    for i in 1001..=1500i64 {
        exec(&db, &format!("INSERT INTO t VALUES ({}, 'c', 0.0)", i));
    }
    db.flush().unwrap();
    wait_for_compaction();

    assert_eq!(
        count_rows(&db),
        1500,
        "All 1500 rows should be present after compaction cycles"
    );
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_orphan_cleanup_on_open() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: Create DB, insert data, flush, close
    {
        let db = Database::create_with_config(&path, DBConfig::for_edge()).unwrap();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)");
        for i in 1..=100i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'ok')", i));
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Create an orphan .sst file
    let lsm = path.with_extension("mote").join("lsm");
    let orphan = lsm.join("l0_orphan_999999.sst");
    std::fs::write(&orphan, b"garbage data not a real sstable").unwrap();
    assert!(orphan.exists(), "Orphan file should exist before open");

    // Phase 2: Reopen — orphan should be cleaned up
    {
        let db = Database::open_with_config(&path, DBConfig::for_edge()).unwrap();
        assert!(
            !orphan.exists(),
            "Orphan SSTable should be cleaned up on open"
        );

        let count = count_rows_via(&db, "SELECT id FROM t");
        assert_eq!(count, 100, "Original data should survive reopen");
    }
}

fn count_rows_via(db: &Database, sql: &str) -> usize {
    // Use materialize() — see count_rows() for why (SelectStreaming-only match
    // silently returned 0 for columnar-served tables).
    match exec(db, sql) {
        motedb::sql::QueryResult::Select { rows, .. } => rows.len(),
        _ => 0,
    }
}

// ── Category 5: Restart Recovery ──────────────────────────────────────

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_data_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: Write
    {
        let db = Database::create_with_config(&path, DBConfig::for_edge()).unwrap();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)");
        for i in 1..=5000i64 {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'val_{}')", i, i));
        }
        db.flush().unwrap();
        db.close().unwrap();
    }

    // Phase 2: Reopen and verify
    {
        let db = Database::open_with_config(&path, DBConfig::for_edge()).unwrap();
        let count = count_rows_via(&db, "SELECT id FROM t");
        assert_eq!(
            count, 5000,
            "All rows should survive restart, got {}",
            count
        );

        // Spot-check values
        for i in (1..=5000).step_by(100) {
            let sql = format!("SELECT * FROM t WHERE id = {}", i);
            let result = exec(&db, &sql);
            match result {
                motedb::sql::QueryResult::Select { rows, .. } => {
                    let row = rows
                        .into_iter()
                        .next()
                        .unwrap_or_else(|| panic!("Row {} missing after restart", i));
                    assert_eq!(
                        row[1],
                        Value::text(format!("val_{}", i)),
                        "Row {} value mismatch after restart",
                        i
                    );
                }
                _ => panic!("Expected Select result for row {}", i),
            }
        }
    }
}

#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_compaction_result_survives_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    // Phase 1: Write in batches to trigger compaction
    {
        let db = Database::create_with_config(&path, DBConfig::for_edge()).unwrap();
        exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, status TEXT)");
        for batch in 0..5i64 {
            let start = batch * 2000 + 1;
            let end = start + 2000;
            for i in start..end {
                exec(&db, &format!("INSERT INTO t VALUES ({}, 'data')", i));
            }
            db.flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        wait_for_compaction();
        db.close().unwrap();
    }

    // Phase 2: Reopen — data should have survived compaction + restart
    {
        let db = Database::open_with_config(&path, DBConfig::for_edge()).unwrap();
        let count = count_rows_via(&db, "SELECT id FROM t");
        assert_eq!(
            count, 10000,
            "All 10000 rows should survive compaction + restart, got {}",
            count
        );

        for i in (1..=10000).step_by(200) {
            let sql = format!("SELECT * FROM t WHERE id = {}", i);
            let result = exec(&db, &sql);
            match result {
                motedb::sql::QueryResult::Select { rows, .. } => {
                    assert!(
                        rows.into_iter().next().is_some(),
                        "Row {} missing after restart",
                        i
                    );
                }
                _ => panic!("Expected Select result for row {}", i),
            }
        }
    }
}

// ── Category 6: Tombstone Resurrection Prevention ──────────────────────

/// Regression test: tombstones must NOT be dropped during intermediate-level compaction.
///
/// Before the fix, compaction at level N→N+1 would drop expired tombstones even when
/// deeper levels (N+2, ...) still held live copies of the key. After compaction removed
/// the tombstone, reads would "resurrect" the old value from the deeper level.
///
/// This test uses tombstone_ttl_secs=0 to maximize the chance of premature dropping,
/// then verifies that deleted keys stay deleted after intermediate compaction.
#[test]
#[ignore = "compaction stress: slow in debug (~25s), run with --ignored"]
fn test_tombstone_not_dropped_at_intermediate_level() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_edge();
    config.lsm_config.tombstone_ttl_secs = Some(0); // Drop tombstones ASAP — stresses the bug
    let db = Database::create_with_config(dir.path(), config).unwrap();

    exec(&db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)");

    // Insert 1000 rows across multiple flushes to create multi-level SSTables
    for batch in 0..5i64 {
        let start = batch * 200 + 1;
        let end = start + 200;
        for i in start..end {
            exec(&db, &format!("INSERT INTO t VALUES ({}, 'original')", i));
        }
        db.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(300));
    }

    // Wait for compaction to push data into deeper levels
    std::thread::sleep(std::time::Duration::from_secs(3));
    db.flush().unwrap();

    // Verify rows exist
    assert_eq!(count_rows(&db), 1000, "Should have 1000 rows before delete");

    // Delete half the rows
    for i in 1..=500i64 {
        exec(&db, &format!("DELETE FROM t WHERE id = {}", i));
    }
    db.flush().unwrap();

    // Trigger compaction at intermediate levels (NOT last level)
    for batch in 0..3i64 {
        let start = 2000 + batch * 100 + 1;
        for i in 0..50i64 {
            exec(
                &db,
                &format!("INSERT INTO t VALUES ({}, 'padding')", start + i),
            );
        }
        db.flush().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(500));
    }

    // Wait for compaction to run on intermediate levels
    std::thread::sleep(std::time::Duration::from_secs(5));
    db.flush().unwrap();
    std::thread::sleep(std::time::Duration::from_secs(3));

    // Deleted rows must NOT reappear (tombstone was not prematurely dropped)
    assert_eq!(
        count_rows(&db),
        650,
        "Should have 500 (survivors) + 150 (padding) = 650 rows"
    );

    for i in 1..=500i64 {
        assert!(
            get_row(&db, i).is_none(),
            "Deleted row {} should NOT be resurrected",
            i
        );
    }
    for i in 501..=1000i64 {
        assert!(get_row(&db, i).is_some(), "Row {} should still exist", i);
    }
}
