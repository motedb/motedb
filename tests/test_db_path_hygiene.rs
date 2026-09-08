//! Database path resolution hygiene.
//!
//! `create(existing_dir)` used to create a SIBLING `{dir}.mote` — so
//! `create(TempDir::new())` leaked a `.mote` directory next to the tempdir
//! that TempDir never cleaned up. Now the database lives INSIDE an existing
//! directory argument. The legacy `{stem}.mote` sibling form still works for
//! non-directory paths, and `store.mote` paths are used verbatim.
use motedb::{DBConfig, Database};
use tempfile::TempDir;

#[test]
fn create_inside_existing_dir_no_sibling_leak() {
    let dir = TempDir::new().unwrap();
    {
        let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        db.execute("INSERT INTO t (id, v) VALUES (1, 10)").unwrap();
        db.close().unwrap();
    }
    // DB files must be INSIDE the tempdir…
    assert!(dir.path().join("lsm").is_dir(), "lsm/ inside the given dir");
    // …and no sibling {dir}.mote directory may exist next to it.
    let mut sibling = dir.path().to_path_buf();
    sibling.set_extension("mote");
    assert!(
        !sibling.exists(),
        "create(existing dir) must not create a sibling {:?}",
        sibling
    );
    // Reopen through the same directory path.
    let db = Database::open(dir.path()).unwrap();
    let r = db
        .execute("SELECT COUNT(*) FROM t")
        .unwrap()
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert!(matches!(rows[0][0], motedb::types::Value::Integer(1)));
    db.close().unwrap();
}

#[test]
fn legacy_sibling_convention_still_works() {
    let dir = TempDir::new().unwrap();
    let stem = dir.path().join("app"); // does NOT exist
    {
        let db = Database::create(&stem).unwrap();
        db.execute("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
        db.close().unwrap();
    }
    let legacy = dir.path().join("app.mote");
    assert!(
        legacy.is_dir(),
        "legacy {stem:?}.mote sibling still honored"
    );
    // Reopen via the legacy stem form AND the explicit .mote form.
    let db = Database::open(&stem).unwrap();
    db.close().unwrap();
    let db = Database::open(&legacy).unwrap();
    db.close().unwrap();
}

#[test]
fn open_rejects_unrelated_empty_dir() {
    let dir = TempDir::new().unwrap();
    let unrelated = dir.path().join("unrelated");
    std::fs::create_dir_all(&unrelated).unwrap();
    // open() on a directory with no database inside must NOT adopt it as a
    // fresh database — it should fail like opening a missing path.
    let err = Database::open(&unrelated);
    assert!(err.is_err(), "open() on an unrelated empty dir must error");
}
