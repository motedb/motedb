//! Upsert semantics: INSERT OR IGNORE / INSERT OR REPLACE /
//! ON CONFLICT (pk) DO NOTHING / DO UPDATE SET ... (with `excluded.`).
//!
//! These tests pin the row-level semantics (skip / replace / update-in-place),
//! affected-row counting, transaction behavior (including upserts that hit
//! rows INSERTed earlier in the same transaction), and the error cases.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn new_db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    (db, dir)
}

fn exec(db: &Database, sql: &str) -> u64 {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap();
    match rs {
        motedb::sql::QueryResult::Modification { affected_rows } => affected_rows as u64,
        motedb::sql::QueryResult::Definition { .. } => 0,
        other => panic!("expected Modification, got {:?} for {}", other, sql),
    }
}

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let rs = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("SQL failed: {}\n  err: {}", sql, e))
        .materialize()
        .unwrap();
    match rs {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        _ => panic!("not Select: {}", sql),
    }
}

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = rows(db, sql);
    assert_eq!(r.len(), 1, "1 row: {}", sql);
    match r[0].first() {
        Some(Value::Integer(n)) => *n,
        o => panic!("int? {:?}: {}", o, sql),
    }
}

fn sensor_db() -> (Database, TempDir) {
    let (db, dir) = new_db();
    exec(
        &db,
        "CREATE TABLE sensors (id INTEGER PRIMARY KEY, name TEXT, reading INTEGER)",
    );
    (db, dir)
}

#[test]
fn test_insert_or_ignore_skips_duplicates() {
    let (db, _dir) = sensor_db();
    assert_eq!(exec(&db, "INSERT INTO sensors VALUES (1, 'temp', 10)"), 1);

    // Duplicate PK → skipped, affected = 0.
    assert_eq!(
        exec(&db, "INSERT OR IGNORE INTO sensors VALUES (1, 'temp', 999)"),
        0
    );
    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 1"),
        10
    );

    // New PK → inserted, affected = 1.
    assert_eq!(
        exec(&db, "INSERT OR IGNORE INTO sensors VALUES (2, 'hum', 20)"),
        1
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM sensors"), 2);
}

#[test]
fn test_insert_or_replace_full_row_replacement() {
    let (db, _dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (1, 'temp', 10)");

    assert_eq!(
        exec(
            &db,
            "INSERT OR REPLACE INTO sensors VALUES (1, 'temp2', 42)"
        ),
        1
    );
    // Every column replaced, row count unchanged.
    assert_eq!(
        rows(&db, "SELECT name, reading FROM sensors WHERE id = 1")[0],
        vec![Value::text("temp2".to_string()), Value::Integer(42)]
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM sensors"), 1);
}

#[test]
fn test_on_conflict_do_nothing() {
    let (db, _dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (1, 'temp', 10)");

    assert_eq!(
        exec(
            &db,
            "INSERT INTO sensors VALUES (1, 'x', 99) ON CONFLICT (id) DO NOTHING"
        ),
        0
    );
    assert_eq!(
        exec(
            &db,
            "INSERT INTO sensors VALUES (2, 'y', 5) ON CONFLICT DO NOTHING"
        ),
        1
    );
    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 1"),
        10
    );
}

#[test]
fn test_on_conflict_do_update_basic() {
    let (db, _dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (1, 'temp', 10)");

    assert_eq!(
        exec(
            &db,
            "INSERT INTO sensors VALUES (1, 'ignored_name', 99) \
             ON CONFLICT (id) DO UPDATE SET reading = excluded.reading"
        ),
        1
    );
    // reading updated from excluded; untouched columns keep old values.
    assert_eq!(
        rows(&db, "SELECT name, reading FROM sensors WHERE id = 1")[0],
        vec![Value::text("temp".to_string()), Value::Integer(99)]
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM sensors"), 1);
}

#[test]
fn test_on_conflict_do_update_accumulates() {
    // The canonical sensor pattern: existing + excluded.
    let (db, _dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (7, 'counter', 100)");

    for delta in [1i64, 2, 3] {
        exec(
            &db,
            &format!(
                "INSERT INTO sensors VALUES (7, 'counter', {d}) \
                 ON CONFLICT (id) DO UPDATE SET reading = reading + excluded.reading",
                d = delta
            ),
        );
    }
    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 7"),
        106
    );
}

#[test]
fn test_upsert_multi_row_mixed_conflicts() {
    let (db, _dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (1, 'temp', 10)");

    let affected = exec(
        &db,
        "INSERT INTO sensors VALUES (1, 'dup', 0), (2, 'new', 5), (1, 'dup2', 0) \
         ON CONFLICT (id) DO UPDATE SET reading = excluded.reading",
    );
    // Both conflicting rows updated + one insert = 3 affected.
    assert_eq!(affected, 3);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM sensors"), 2);
    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 1"),
        0
    );
}

#[test]
fn test_upsert_inside_transaction_hits_same_txn_insert() {
    let (db, _dir) = sensor_db();
    exec(&db, "BEGIN");
    // Row 5 exists only in this transaction's write_set.
    exec(&db, "INSERT INTO sensors VALUES (5, 'mid', 1)");
    // Upsert must find the uncommitted row and update it.
    exec(
        &db,
        "INSERT INTO sensors VALUES (5, 'mid', 41) \
         ON CONFLICT (id) DO UPDATE SET reading = reading + excluded.reading",
    );
    exec(&db, "COMMIT");

    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 5"),
        42
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM sensors"), 1);
}

#[test]
fn test_upsert_transaction_rollback() {
    let (db, _dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (1, 'temp', 10)");

    exec(&db, "BEGIN");
    exec(
        &db,
        "INSERT INTO sensors VALUES (1, 'x', 999) \
         ON CONFLICT (id) DO UPDATE SET reading = excluded.reading",
    );
    exec(&db, "INSERT OR REPLACE INTO sensors VALUES (2, 'gone', 1)");
    exec(&db, "ROLLBACK");

    // Neither the update nor the replace survived.
    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 1"),
        10
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM sensors"), 1);
}

#[test]
fn test_upsert_errors() {
    let (db, _dir) = new_db();
    // No PK table.
    exec(&db, "CREATE TABLE nopk (a INTEGER, b TEXT)");
    assert!(db
        .execute("INSERT INTO nopk VALUES (1, 'x') ON CONFLICT (a) DO UPDATE SET b = 'y'")
        .is_err());
    assert!(db
        .execute("INSERT INTO nopk VALUES (1, 'x') ON CONFLICT DO UPDATE SET b = 'y'")
        .is_err());
    // Wrong target column (b is not the PK).
    exec(&db, "CREATE TABLE withpk (id INTEGER PRIMARY KEY, b TEXT)");
    assert!(db
        .execute("INSERT INTO withpk VALUES (1, 'x') ON CONFLICT (b) DO UPDATE SET b = 'y'")
        .is_err());
    // Unknown column in SET (errors once a conflicting row exists — with no
    // conflict the statement just inserts, matching SQLite).
    exec(&db, "INSERT INTO withpk VALUES (1, 'x')");
    assert!(db
        .execute("INSERT INTO withpk VALUES (1, 'y') ON CONFLICT (id) DO UPDATE SET zz = 1")
        .is_err());
    // OR REPLACE on TimeSeries table is rejected.
    exec(
        &db,
        "CREATE TABLE ts_metrics (ts TIMESTAMP, value FLOAT) TIMESERIES(ts)",
    );
    assert!(db
        .execute("INSERT OR REPLACE INTO ts_metrics VALUES (1, 1.0)")
        .is_err());
    // No-PK tables still accept the targetless actions (plain insert).
    assert_eq!(exec(&db, "INSERT OR IGNORE INTO nopk VALUES (1, 'x')"), 1);
}

#[test]
fn test_upsert_auto_increment_allocates_on_insert() {
    let (db, _dir) = new_db();
    exec(
        &db,
        "CREATE TABLE auto_t (id INTEGER PRIMARY KEY AUTO_INCREMENT, v INTEGER)",
    );
    exec(&db, "INSERT INTO auto_t (v) VALUES (1)");
    exec(
        &db,
        "INSERT INTO auto_t (v) VALUES (2) ON CONFLICT DO NOTHING",
    );
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM auto_t"), 2);
    // Explicit-PK upsert still works on AUTO_INCREMENT tables.
    assert_eq!(
        exec(
            &db,
            "INSERT INTO auto_t VALUES (1, 100) \
             ON CONFLICT (id) DO UPDATE SET v = excluded.v"
        ),
        1
    );
    assert_eq!(scalar_i64(&db, "SELECT v FROM auto_t WHERE id = 1"), 100);
    assert_eq!(scalar_i64(&db, "SELECT COUNT(*) FROM auto_t"), 2);
}

#[test]
fn test_context_sensitive_keywords_still_usable() {
    // `replace` stays a function name; `conflict`/`nothing` stay valid
    // column names (they are NOT reserved keywords).
    let (db, _dir) = new_db();
    exec(
        &db,
        "CREATE TABLE words (conflict TEXT, nothing INTEGER, todo TEXT)",
    );
    exec(&db, "INSERT INTO words VALUES ('a', 1, 'b')");
    assert_eq!(
        rows(
            &db,
            "SELECT replace(conflict, 'a', 'z'), nothing FROM words"
        )[0],
        vec![Value::text("z".to_string()), Value::Integer(1)]
    );
}

#[test]
fn test_upsert_durability_across_reopen() {
    let (db, dir) = sensor_db();
    exec(&db, "INSERT INTO sensors VALUES (3, 'd', 1)");
    exec(
        &db,
        "INSERT INTO sensors VALUES (3, 'd', 2) \
         ON CONFLICT (id) DO UPDATE SET reading = reading + excluded.reading",
    );
    exec(&db, "INSERT OR REPLACE INTO sensors VALUES (9, 'r', 5)");
    let path = dir.path().to_path_buf();
    drop(db);

    let db = Database::open(&path).unwrap();
    assert_eq!(
        scalar_i64(&db, "SELECT reading FROM sensors WHERE id = 3"),
        3
    );
    assert_eq!(
        rows(&db, "SELECT name FROM sensors WHERE id = 9")[0],
        vec![Value::text("r".to_string())]
    );
}
