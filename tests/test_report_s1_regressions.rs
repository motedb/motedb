//! Regression tests for BUG-REPORT-0.16.0 S1 items B-02 / B-16 / B-03 / B-17.
//!
//! B-02/B-16 share one root: `ALTER TABLE ADD COLUMN` widens the schema (and
//! the segment builder), but rows written from the OLD width — a stored row
//! re-UPDATEd, buffered into the write buffer by an in-transaction UPDATE —
//! were narrower. B-02 failed validation ("Column count mismatch: expected
//! N, got N-1"); B-16 panicked in `flush_buffer_locked` at close, aborting
//! the process mid-flush and stranding a directory that could not be
//! reopened. The B-16 acceptance runs in a SUBPROCESS (an abort would take
//! down the whole test process) and includes the reopen step.

use motedb::types::Value;
use motedb::{Database, QueryResult};
use std::process::Command;
use std::process::Stdio;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        QueryResult::Select { rows, .. } => rows,
        other => panic!("unexpected: {other:?}"),
    }
}

/// B-02: UPDATE of an in-memory (same-session) row after ALTER ADD COLUMN.
#[test]
fn alter_then_update_same_session_row() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, emb VECTOR(4), tag TEXT)")
        .unwrap();
    db.execute("INSERT INTO c VALUES (1, '[1.0,2.0,3.0,4.0]', 'a')")
        .unwrap();
    db.execute("ALTER TABLE c ADD COLUMN extra TEXT").unwrap();

    let r = db.execute("UPDATE c SET tag = 'b' WHERE id = 1");
    assert!(r.is_ok(), "B-02: UPDATE after ALTER failed: {:?}", r.err());

    // The updated row also gains the ALTER's DEFAULT (NULL when none).
    assert_eq!(
        rows(&db, "SELECT id, tag, extra FROM c"),
        vec![vec![
            Value::Integer(1),
            Value::Text("b".into()),
            Value::Null
        ]]
    );

    // And the update survives flush + reopen (B-16's non-tx sibling).
    db.checkpoint().unwrap();
    drop(db);
    let db = Database::open(dir.path().join("d.mote")).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, tag, extra FROM c"),
        vec![vec![
            Value::Integer(1),
            Value::Text("b".into()),
            Value::Null
        ]]
    );
}

/// The B-16 child: ALTER → in-transaction UPDATE → COMMIT → close. The old
/// code aborted (exit 134) inside close's destructor with a half-flushed
/// buffer. Child mode of this same test binary; DB path via env.
#[test]
fn b16_child_mode() {
    if std::env::var("MOTEDB_B16_CHILD_DB").is_err() {
        return; // not in child mode
    }
    let db_path = std::env::var("MOTEDB_B16_CHILD_DB").unwrap();
    let _ = std::fs::remove_dir_all(format!("{db_path}.mote"));
    let db = Database::create(db_path.as_str()).unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, emb VECTOR(4), tag TEXT)")
        .unwrap();
    db.execute("INSERT INTO c VALUES (1, '[1.0,2.0,3.0,4.0]', 'a')")
        .unwrap();
    db.execute("ALTER TABLE c ADD COLUMN extra TEXT DEFAULT 'seed'")
        .unwrap();

    let tx = db.begin_transaction().unwrap();
    db.execute("UPDATE c SET tag = 'b' WHERE id = 1").unwrap();
    db.commit_transaction(tx).unwrap();

    // Communicate via exit code: the cargo harness swallows a passing
    // test's stdout, so println markers never reach the parent.
    match db.close() {
        Ok(()) => std::process::exit(0),
        Err(e) => {
            eprintln!("close -> Err: {e}");
            std::process::exit(3);
        }
    }
}

/// B-16: the whole txn → commit → close sequence must survive in a
/// subprocess, and the database must reopen with the committed data.
#[test]
fn alter_txn_update_commit_close_reopens_clean() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("b16");

    let out = Command::new(std::env::current_exe().expect("current exe"))
        .arg("b16_child_mode")
        .arg("--exact")
        .env("MOTEDB_B16_CHILD_DB", &db_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn B-16 child")
        .wait_with_output()
        .expect("wait B-16 child");

    assert_eq!(
        out.status.code(),
        Some(0),
        "B-16: child did not close cleanly ({}) — the old code aborted here (SIGABRT, exit 134)",
        out.status
    );

    // Reopen: committed txn data must be there, nothing bricked.
    let db = Database::open(format!("{}.mote", db_path.display())).unwrap();
    assert_eq!(
        rows(&db, "SELECT id, tag, extra FROM c"),
        vec![vec![
            Value::Integer(1),
            Value::Text("b".into()),
            Value::Text("seed".into())
        ]]
    );
}

/// B-03: TIMESTAMP ± INTEGER with the timestamp spelled as an ISO TEXT
/// literal (or DATE) — the literal-side arithmetic used to fold to NULL and
/// the predicate silently matched nothing.
#[test]
fn timestamp_arithmetic_with_iso_text_literal() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (1, '2026-01-01 00:00:00')")
        .unwrap();
    db.execute("INSERT INTO ev VALUES (2, '2026-01-01 00:10:00')")
        .unwrap();
    db.checkpoint().unwrap();

    let count = |sql: &str| rows(&db, sql).len();
    assert_eq!(
        count("SELECT id FROM ev WHERE ts >= '2026-01-01 00:00:00' + 60000000"),
        1
    );
    assert_eq!(
        count("SELECT id FROM ev WHERE '2026-01-01 00:00:00' + 60000000 <= ts"),
        1
    );
    assert_eq!(
        count("SELECT id FROM ev WHERE ts >= '2026-01-01 00:00:00' - 60000000"),
        2
    );
    assert_eq!(
        count("SELECT id FROM ev WHERE ts < '2026-01-01 00:00:00' + 60000000"),
        1
    );
    assert_eq!(
        count("SELECT id FROM ev WHERE ts + 60000000 >= '2026-01-01 00:01:00'"),
        2
    );
    assert_eq!(
        count("SELECT id FROM ev WHERE ts BETWEEN '2026-01-01 00:00:00' - 1 AND '2026-01-01 00:00:00' + 1"),
        1
    );
    // Non-timestamp text stays benign (NULL compare → no match), not an error.
    assert_eq!(
        count("SELECT id FROM ev WHERE ts >= 'not a timestamp' + 60000000"),
        0
    );
}

/// B-17: positional INSERT may omit trailing columns; they take their
/// DEFAULT (or NULL), as in PostgreSQL/SQLite. An explicit column list and
/// too-many-values still require exact counts; NOT NULL without a default
/// still rejects.
#[test]
fn positional_insert_fills_trailing_defaults() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("d.mote")).unwrap();
    db.execute("CREATE TABLE h (id INT PRIMARY KEY, name TEXT, lvl INT DEFAULT 7, note TEXT)")
        .unwrap();

    db.execute("INSERT INTO h VALUES (1, 'a')").unwrap();
    db.execute("INSERT INTO h VALUES (2, 'b', 9)").unwrap();
    db.execute("INSERT INTO h VALUES (3, 'c', NULL, 'n')")
        .unwrap();
    assert_eq!(
        rows(&db, "SELECT id, lvl, note FROM h ORDER BY id"),
        vec![
            vec![Value::Integer(1), Value::Integer(7), Value::Null],
            vec![Value::Integer(2), Value::Integer(9), Value::Null],
            vec![Value::Integer(3), Value::Null, Value::Text("n".into())],
        ]
    );

    let too_many = db.execute("INSERT INTO h VALUES (4, 'd', 1, 2, 3)");
    assert!(too_many.is_err(), "more values than columns must error");
    let explicit_short = db.execute("INSERT INTO h (id, name) VALUES (5, 'e')");
    assert!(
        explicit_short.is_ok(),
        "column-list INSERT with all listed columns is exact: {:?}",
        explicit_short.err()
    );
    let col_mismatch = db.execute("INSERT INTO h (id, name) VALUES (6)");
    assert!(
        col_mismatch.is_err(),
        "column-list count mismatch must error"
    );

    db.execute("CREATE TABLE s (id INT PRIMARY KEY, req TEXT NOT NULL)")
        .unwrap();
    let not_null = db.execute("INSERT INTO s VALUES (1)");
    assert!(
        not_null.is_err(),
        "NOT NULL without default must still reject"
    );
}

/// B-16 residue: a directory bricked by 0.16.0's crash-at-close holds
/// segment files whose row_map entries were never appended — rows with
/// key=0/ts=0 (reproduced with the real 0.16.0 binary: 2 such files plus
/// the real data). Key `0` can never belong to a table (keys are
/// `(table_id << 32) | row_id`), so newest-wins dedup never collapsed the
/// ghost against the real row: every full scan / COUNT(*) / GROUP BY showed
/// one extra row while the PK point read was fine, and a checkpoint merged
/// the ghost INTO the real segment permanently. Open now detects foreign
/// keys and heals the table with one filtered merge.
#[test]
fn bricked_directory_ghost_rows_are_dropped_on_open() {
    use motedb::storage::lsm::ColumnarSSTableBuilder;
    use motedb::types::ColumnType;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("b16r.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE c (id INT PRIMARY KEY, tag TEXT)")
            .unwrap();
        for i in 1..=5 {
            db.execute(&format!("INSERT INTO c VALUES ({i}, 'a{i}')"))
                .unwrap();
        }
        db.checkpoint().unwrap();
        db.close().unwrap();
    }
    // Plant the crash residue exactly as found on disk: complete, parseable
    // orphan segment files (ids the manifest never saw) whose only row has
    // key=0/ts=0 and the torn column values of the crashing UPDATE.
    let seg_dir = path.join("columnar_ms").join("c");
    for id in [40u64, 41] {
        let mut b = ColumnarSSTableBuilder::new(
            seg_dir.join(format!("{id:010}.sst")),
            vec![ColumnType::Integer, ColumnType::Text],
        );
        b.add_values(
            0,
            0,
            false,
            &[Value::Integer(1), Value::Text("ghost".into())],
        )
        .unwrap();
        b.finish().unwrap();
    }

    let expect = |db: &Database, phase: &str| {
        assert_eq!(
            rows(db, "SELECT id, tag FROM c ORDER BY id"),
            (1..=5)
                .map(|i| vec![Value::Integer(i), Value::Text(format!("a{i}").into())])
                .collect::<Vec<_>>(),
            "{phase}: full scan"
        );
        assert_eq!(
            rows(db, "SELECT COUNT(*) FROM c")[0][0],
            Value::Integer(5),
            "{phase}: COUNT(*)"
        );
        assert_eq!(
            rows(db, "SELECT id FROM c WHERE tag = 'ghost'").len(),
            0,
            "{phase}: ghost row must be invisible"
        );
        assert_eq!(
            rows(db, "SELECT tag, COUNT(*) FROM c GROUP BY tag").len(),
            5,
            "{phase}: GROUP BY"
        );
        assert_eq!(
            rows(db, "SELECT id FROM c WHERE id = 1").len(),
            1,
            "{phase}: PK read"
        );
    };

    let db = Database::open(&path).unwrap();
    expect(&db, "after open");
    // A checkpoint (merge) must not bring the ghost back, and neither must
    // a reopen — the on-disk table is healed, not just filtered.
    db.checkpoint().unwrap();
    expect(&db, "after checkpoint");
    drop(db);
    let db = Database::open(&path).unwrap();
    expect(&db, "after reopen");
    db.execute("INSERT INTO c VALUES (6, 'a6')").unwrap();
    assert_eq!(
        rows(&db, "SELECT COUNT(*) FROM c")[0][0],
        Value::Integer(6),
        "writes keep working on the healed table"
    );
}
