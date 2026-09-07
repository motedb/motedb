//! Savepoint read-your-writes window: queries between ROLLBACK TO and COMMIT
//! must not count phantom rows. Found by SQLite differential testing — the
//! write_set read-adjustment added ws.len() to filtered COUNTs, and undo
//! replays left stale index entries, producing counts like 7/8 for tables
//! holding exactly 1 matching row.
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn count(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    r.select_rows()
        .and_then(|(_, rows)| {
            rows.first().and_then(|r| r.first().cloned()).and_then(|v| {
                if let motedb::types::Value::Integer(i) = v {
                    Some(i)
                } else {
                    None
                }
            })
        })
        .unwrap_or(-1)
}

fn seed(db: &Database) {
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, a INT, c TEXT)")
        .unwrap();
    for i in 1..=30i64 {
        db.execute(&format!(
            "INSERT INTO t (id, a, c) VALUES ({i}, {}, 't{}')",
            i % 21,
            i % 6
        ))
        .unwrap();
    }
    db.execute("CHECKPOINT").unwrap();
}

#[test]
fn no_phantom_counts_in_rollback_to_window() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    seed(&db);

    // Txn1: committed insert.
    for s in ["BEGIN", "INSERT INTO t (id, a, c) VALUES (91, 5, 'tx')"] {
        db.execute(s).unwrap();
    }
    db.execute("COMMIT").unwrap();
    // Txn2: fully rolled back.
    for s in [
        "BEGIN",
        "INSERT INTO t (id, a, c) VALUES (92, 6, 'ty')",
        "UPDATE t SET a = 99 WHERE id <= 3",
        "DELETE FROM t WHERE id > 28",
    ] {
        db.execute(s).unwrap();
    }
    db.execute("ROLLBACK").unwrap();
    // Txn3: insert pre-savepoint, DELETE+UPDATE post-savepoint, ROLLBACK TO.
    for s in [
        "BEGIN",
        "INSERT INTO t (id, a, c) VALUES (93, 7, 'tz')",
        "SAVEPOINT sp1",
        "DELETE FROM t WHERE id BETWEEN 10 AND 15",
        "UPDATE t SET a = 55 WHERE id = 1",
    ] {
        db.execute(s).unwrap();
    }
    db.execute("ROLLBACK TO sp1").unwrap();

    // ── In-window assertions (before COMMIT) ──
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 32, "in-window total");
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id = 93"),
        1,
        "in-window point count must not include write_set phantoms"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id = 1"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id = 92"), 0);
    assert_eq!(
        count(&db, "SELECT a FROM t WHERE id = 1"),
        1,
        "update undone"
    );

    db.execute("COMMIT").unwrap();

    // ── Post-commit assertions ──
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t"), 32);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id = 93"), 1);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id = 92"), 0);
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id = 1"), 1);
    assert_eq!(
        count(&db, "SELECT a FROM t WHERE id = 1"),
        1,
        "old value restored"
    );
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t WHERE id BETWEEN 10 AND 15"),
        6,
        "savepoint delete undone"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id > 28"), 4);
}

#[test]
fn update_undo_leaves_no_duplicate_rows() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    seed(&db);
    for s in [
        "BEGIN",
        "SAVEPOINT s1",
        "UPDATE t SET a = 55 WHERE id = 1",
        "ROLLBACK TO s1",
    ] {
        db.execute(s).unwrap();
    }
    db.execute("COMMIT").unwrap();
    assert_eq!(
        count(&db, "SELECT COUNT(*) FROM t"),
        30,
        "no duplicate rows"
    );
    assert_eq!(count(&db, "SELECT COUNT(*) FROM t WHERE id = 1"), 1);
    assert_eq!(count(&db, "SELECT a FROM t WHERE id = 1"), 1, "old value");

    // Durable across reopen.
    drop(db);
    let db2 = Database::open_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    assert_eq!(count(&db2, "SELECT COUNT(*) FROM t"), 30);
    assert_eq!(count(&db2, "SELECT COUNT(*) FROM t WHERE id = 1"), 1);
}
