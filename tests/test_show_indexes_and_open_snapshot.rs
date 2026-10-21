//! Regressions from the 2026-10-22 functional-test round:
//!
//! 1. `Database::open_snapshot` into a NON-`.mote` destination — the open
//!    path resolver's directory markers were legacy-only (`lsm/`||`wal/`),
//!    so a freshly unpacked P1-layout snapshot (no wal/ until first write)
//!    fell through to the sibling-`.mote` rule and opened a nonexistent
//!    path (ENOENT).
//! 2. `unpack_snapshot` accepting an existing EMPTY destination dir (the
//!    natural `mkdir dest && import` shape) while still rejecting non-empty.
//! 3. `SHOW INDEXES [FROM t]` reading the authoritative index registry.

use motedb::Database;

#[test]
fn open_snapshot_into_non_mote_dest() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("src.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v FLOAT)")
        .unwrap();
    for i in 0..10i64 {
        db.execute_prepared(
            "INSERT INTO t VALUES (?, ?)",
            vec![
                motedb::types::Value::Integer(i),
                motedb::types::Value::Float(i as f64 / 4.0),
            ],
        )
        .unwrap();
    }
    db.checkpoint().unwrap();

    let snap = dir.path().join("snap.mote");
    db.export_snapshot(&snap, false).unwrap();

    // Non-.mote dest: used to ENOENT via the sibling-.mote rule.
    let dest = dir.path().join("unpacked");
    let db2 = Database::open_snapshot(&snap, &dest).unwrap();
    let n = db2.query("SELECT COUNT(*) FROM t").unwrap();
    assert_eq!(n[0][0], motedb::types::Value::Integer(10));
    let _ = db2.close();

    // Pre-created EMPTY dest dir is accepted; non-empty is rejected.
    let dest2 = dir.path().join("unpacked2");
    std::fs::create_dir_all(&dest2).unwrap();
    let db3 = Database::open_snapshot(&snap, &dest2).unwrap();
    let _ = db3.close();
    let dest3 = dir.path().join("unpacked3");
    std::fs::create_dir_all(dest3.join("junk")).unwrap();
    let err = match Database::open_snapshot(&snap, &dest3) {
        Err(e) => e,
        Ok(_) => panic!("expected non-empty destination rejection"),
    };
    assert!(err.to_string().contains("not empty"), "{err}");
    let _ = db.close();
}

#[test]
fn show_indexes_lists_registry_entries() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::create(dir.path().join("si.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, emb VECTOR(4))")
        .unwrap();
    db.execute("CREATE INDEX t_name ON t (name) USING COLUMN")
        .unwrap();
    db.execute("CREATE TEXT INDEX t_fts ON t (name)").unwrap();
    db.execute("CREATE VECTOR INDEX t_vec ON t (emb)").unwrap();

    let rows = db.query("SHOW INDEXES FROM t").unwrap();
    let names: Vec<String> = rows
        .iter()
        .map(|r| match &r[1] {
            motedb::types::Value::Text(s) => s.as_str().to_string(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(names.len(), 3, "{names:?}");
    for expected in ["t_name", "t_fts", "t_vec"] {
        assert!(names.iter().any(|n| n == expected), "{names:?}");
    }

    // Unfiltered form covers every table (t's three + none on others).
    let all = db.query("SHOW INDEXES").unwrap();
    assert_eq!(all.len(), 3);
    let _ = db.close();
}
