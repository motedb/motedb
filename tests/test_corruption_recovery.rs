//! On-disk corruption self-healing.
//!
//! Edge devices lose power mid-write; corrupted secondary-index files must
//! degrade to a REBUILD, not a permanently missing (or silently empty)
//! index. Catalog corruption must produce a clear error, not a panic.

use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    match db.execute(sql).unwrap().materialize().unwrap() {
        motedb::sql::QueryResult::Select { rows, .. } => rows,
        other => panic!("expected Select, got {:?} for {}", other, sql),
    }
}

#[test]
fn test_corrupt_column_index_self_rebuilds() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v FLOAT)")
            .unwrap();
        db.execute("CREATE INDEX t_v ON t (v)").unwrap();
        for i in 0..10 {
            db.execute(&format!("INSERT INTO t VALUES ({0}, {0}.5)", i))
                .unwrap();
        }
        let _ = db.close();
    }
    std::fs::write(p.join("indexes/column_t_v.idx"), b"JUNK").unwrap();

    let db = Database::open(&p).unwrap();
    let r = db.query_by_column("t", "v", &Value::Float(3.5)).unwrap();
    assert_eq!(r.len(), 1, "corrupt column index must self-rebuild");
}

#[test]
fn test_corrupt_text_index_self_rebuilds_and_persists() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE d (id INTEGER PRIMARY KEY, body TEXT)")
            .unwrap();
        db.execute("CREATE TEXT INDEX d_body ON d (body)").unwrap();
        for i in 0..10 {
            db.execute(&format!("INSERT INTO d VALUES ({0}, 'word{0} tail{0}')", i))
                .unwrap();
        }
        let _ = db.close();
    }
    // Truncated postings: smaller than a superblock frame — the btree would
    // load as EMPTY with no error (silently swallowing all searches).
    std::fs::write(
        p.join("indexes/text_d_body.fts.d/postings.gbtree"),
        b"CORRUPT",
    )
    .unwrap();

    let db = Database::open(&p).unwrap();
    let r = db.text_search_ranked("d_body", "tail5", 20).unwrap();
    assert_eq!(r.len(), 1, "corrupt text index must self-rebuild");
    // The rebuilt index must PERSIST (checkpoint used to early-return with
    // zero pending updates and an empty WAL, discarding the rebuild).
    drop(db);
    let db = Database::open(&p).unwrap();
    let r = db.text_search_ranked("d_body", "tail5", 20).unwrap();
    assert_eq!(r.len(), 1, "rebuilt text index must survive reopen");
}

#[test]
fn test_corrupt_catalog_clear_error() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .unwrap();
        let _ = db.close();
    }
    std::fs::write(p.join("catalog.bin"), b"NOT BINCODE").unwrap();
    let err = Database::open(&p)
        .err()
        .expect("corrupt catalog must error");
    assert!(
        err.to_string().to_lowercase().contains("serial"),
        "clear serialization error, got: {err}"
    );
}

#[test]
fn test_double_open_rejected_and_lock_released() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    let db = Database::create(&p).unwrap();
    assert!(
        Database::open(&p).is_err(),
        "second open while held must fail"
    );
    drop(db);
    assert!(Database::open(&p).is_ok(), "reopen after drop must succeed");
}
