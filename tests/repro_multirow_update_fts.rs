//! Multi-row UPDATE × FTS: only rows the UPDATE touched may change their
//! match sets. Rows 0..149 become "gamma delta"; rows 150..299 keep
//! "alpha beta {i}". A stale index (lost term tombstones) would return
//! alpha=300 (phantoms) or gamma<150 (missing).
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn n(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    match &rows[0][0] {
        motedb::types::Value::Integer(i) => *i,
        other => panic!("{other:?}"),
    }
}

#[test]
fn multirow_update_fts_exact_matches() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_testing();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path().join("x.mote"), config).unwrap();
    db.execute("CREATE TABLE c (id INTEGER PRIMARY KEY, v TEXT)")
        .unwrap();
    db.execute("CREATE TEXT INDEX ci ON c(v)").unwrap();
    for i in 0..300 {
        db.execute(&format!("INSERT INTO c VALUES ({i}, 'alpha beta {i}')"))
            .unwrap();
    }
    // Checkpoint so the auto-flush threshold fires for the insert postings.
    db.checkpoint().unwrap();
    let pre = n(&db, "SELECT COUNT(*) FROM c WHERE MATCH(v, 'alpha')");
    assert_eq!(pre, 300, "pre-update sanity");

    db.execute("UPDATE c SET v = 'gamma delta' WHERE id < 150")
        .unwrap();

    let post = n(&db, "SELECT COUNT(*) FROM c WHERE MATCH(v, 'alpha')");
    let gamma = n(&db, "SELECT COUNT(*) FROM c WHERE MATCH(v, 'gamma')");
    println!("post-update: alpha={post} (want 150) gamma={gamma} (want 150)");
    assert_eq!(gamma, 150, "updated rows must match the new text");
    assert_eq!(
        post, 150,
        "only untouched rows may still match the old text"
    );
}
