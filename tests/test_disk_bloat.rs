//! Disk-bloat regression tests (BUG #46).
//!
//! DELETE used to write a legacy-columnar tombstone carrying the FULL old row
//! into `columnar_write_bufs` (INSERT/UPDATE had the ColSegmentStore guard,
//! DELETE didn't). VACUUM's step 3a then finished that buffer into
//! `indexes/{table}_col.sst` — a complete corpse file of every deleted row.
//! Net effect: disk after VACUUM = live data + full corpse of deleted rows
//! (measured: delete half → VACUUM → disk GREW 85%; a 500K-row table kept a
//! 12MB corpse after deleting 250K rows).

use motedb::types::Value;
use motedb::Database;
use std::path::Path;
use tempfile::TempDir;

fn rows(r: motedb::StreamingQueryResult) -> Vec<Vec<Value>> {
    use motedb::QueryResult;
    match r.materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        _ => panic!("expected select"),
    }
}

fn count(db: &Database, sql: &str) -> i64 {
    match &rows(db.execute(sql).unwrap())[0][0] {
        Value::Integer(n) => *n,
        o => panic!("expected int, got {o:?}"),
    }
}

fn dir_size(p: &Path) -> u64 {
    std::fs::read_dir(p)
        .map(|rd| {
            rd.flatten()
                .map(|e| match e.metadata() {
                    Ok(m) if m.is_dir() => dir_size(&e.path()),
                    Ok(m) => m.len(),
                    Err(_) => 0,
                })
                .sum()
        })
        .unwrap_or(0)
}

fn db_dir(p: &Path) -> std::path::PathBuf {
    p.with_extension("mote")
}

fn insert_rows(db: &Database, n: i64) {
    for c in 0..(n / 1000) {
        let vals: Vec<String> = (0..1000)
            .map(|i| format!("({}, {}, {:.2})", c * 1000 + i, i % 7, (i as f64) * 0.5))
            .collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(", ")))
            .unwrap();
    }
}

#[test]
fn vacuum_after_deletes_leaves_no_corpse_file() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v REAL)")
        .unwrap();
    insert_rows(&db, 30_000);
    db.vacuum().unwrap();
    let full = dir_size(&db_dir(dir.path()));
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 30_000);

    // delete half
    for i in 0..15_000i64 {
        db.execute(&format!("DELETE FROM t WHERE id = {}", i * 2))
            .unwrap();
    }
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 15_000);

    let pre_vac = dir_size(&db_dir(dir.path()));
    db.vacuum().unwrap();

    // correctness: no resurrection, survivors intact
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 15_000);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id % 2 = 0"), 0);
    assert!(matches!(
        rows(db.execute("SELECT v FROM t WHERE id = 29999").unwrap())[0][0],
        Value::Float(_)
    ));

    let post_vac = dir_size(&db_dir(dir.path()));
    // half the live rows → the compacted store must be at most the pre-vacuum
    // size, and roughly half of the full-table compacted size (was: full+corpse)
    assert!(
        post_vac <= pre_vac,
        "VACUUM must not grow disk: pre={pre_vac} post={post_vac}"
    );
    assert!(
        post_vac <= full * 3 / 4,
        "post-VACUUM disk {post_vac}B should be ≈ half of full-table {full}B, not full+corpse"
    );
    // the legacy corpse file must not exist for ColSegmentStore tables
    let corpse = db_dir(dir.path()).join("indexes").join("t_col.sst");
    assert!(
        !corpse.exists() || corpse.metadata().map(|m| m.len()).unwrap_or(0) < 4096,
        "indexes/t_col.sst corpse file must not materialize (BUG #46)"
    );
}

#[test]
fn vacuum_after_update_churn_reclaims() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, k INT, v REAL)")
        .unwrap();
    insert_rows(&db, 20_000);
    db.vacuum().unwrap();
    let base = dir_size(&db_dir(dir.path()));

    // 8K updates = tombstone+append churn
    for i in 0..8_000i64 {
        db.execute(&format!(
            "UPDATE t SET v = v + 1 WHERE id = {}",
            (i * 17) % 20_000
        ))
        .unwrap();
    }
    let bloated = dir_size(&db_dir(dir.path()));
    db.vacuum().unwrap();
    let reclaimed = dir_size(&db_dir(dir.path()));

    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 20_000);
    assert!(bloated > base, "churn must bloat (sanity)");
    assert!(
        reclaimed <= bloated,
        "VACUUM must reclaim churn: {reclaimed} > {bloated}"
    );
}
