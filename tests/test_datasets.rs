//! Dataset snapshots (v0.14 W7): COMMIT DATASET pins the segment set,
//! compaction GC must not delete pinned files, EXPORT DATASET materializes
//! the snapshot into a fresh standalone DB, DROP DATASET releases the pin.
use motedb::Database;
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    db.execute(sql)
        .unwrap()
        .materialize()
        .unwrap()
        .select_rows()
        .unwrap()
        .1
        .to_vec()
}

fn one(db: &Database, sql: &str) -> Value {
    rows(db, sql)[0][0].clone()
}

use motedb::types::Value;

#[test]
fn dataset_snapshot_isolation_and_gc_protection() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i))
                .unwrap();
        }
        // v1 snapshot: rows 0..100.
        let r = db
            .execute("COMMIT DATASET 'v1' NOTE 'first hundred'")
            .unwrap();
        let _ = r;

        // Mutate: more rows + UPDATE + DELETE, then CHECKPOINT (compaction
        // runs; without the GC filter this would delete v1's segments).
        for i in 100..150 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i))
                .unwrap();
        }
        db.execute("UPDATE t SET v = 'mutated' WHERE id < 10")
            .unwrap();
        db.execute("DELETE FROM t WHERE id >= 90 AND id < 100")
            .unwrap();
        db.checkpoint().unwrap();

        // v2 snapshot: the mutated world.
        db.execute("COMMIT DATASET 'v2'").unwrap();

        // Snapshot isolation via EXPORT: v1 must be exactly the original
        // 100 rows with pre-mutation values.
        let dest1 = dir.path().join("v1.mote");
        let dest1s = dest1.to_str().unwrap().to_string();
        db.execute(&format!("EXPORT DATASET 'v1' TO '{}'", dest1s))
            .unwrap();
        let e1 = Database::open(&dest1).unwrap();
        assert_eq!(one(&e1, "SELECT COUNT(*) FROM t"), Value::Integer(100));
        assert_eq!(
            one(&e1, "SELECT COUNT(*) FROM t WHERE v = 'mutated'"),
            Value::Integer(0),
            "v1 must not contain post-snapshot mutations"
        );
        assert_eq!(
            one(&e1, "SELECT v FROM t WHERE id = 5"),
            Value::text("v5".to_string())
        );
        e1.close().unwrap();

        // v2 reflects the mutations.
        let dest2 = dir.path().join("v2.mote");
        let dest2s = dest2.to_str().unwrap().to_string();
        db.execute(&format!("EXPORT DATASET 'v2' TO '{}'", dest2s))
            .unwrap();
        let e2 = Database::open(&dest2).unwrap();
        assert_eq!(one(&e2, "SELECT COUNT(*) FROM t"), Value::Integer(140));
        assert_eq!(
            one(&e2, "SELECT COUNT(*) FROM t WHERE v = 'mutated'"),
            Value::Integer(10)
        );
        assert_eq!(
            one(&e2, "SELECT COUNT(*) FROM t WHERE id >= 90 AND id < 100"),
            Value::Integer(0)
        );
        e2.close().unwrap();

        // Introspection.
        let ds = rows(
            &db,
            "SELECT dataset, COUNT(*) FROM _dataset_commits GROUP BY dataset",
        );
        assert_eq!(ds.len(), 2);
    }
}

#[test]
fn dataset_refs_survive_reopen_and_gc() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("r.mote");
    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
            .unwrap();
        for i in 0..200 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'v{}')", i, i))
                .unwrap();
        }
        db.execute("COMMIT DATASET 'snap'").unwrap();
        // Force new segments + compaction churn.
        for i in 200..400 {
            db.execute(&format!("INSERT INTO t VALUES ({}, 'w{}')", i, i))
                .unwrap();
        }
        db.checkpoint().unwrap();
    } // clean close

    // Reopen: refs reload from the manifest; compaction after reopen must
    // still protect pinned files.
    let db = Database::open(&path).unwrap();
    for i in 400..600 {
        db.execute(&format!("INSERT INTO t VALUES ({}, 'x{}')", i, i))
            .unwrap();
    }
    db.checkpoint().unwrap();
    db.execute("VACUUM").unwrap(); // aggressive compaction + WAL truncation

    let dest = dir.path().join("out.mote");
    let dests = dest.to_str().unwrap().to_string();
    db.execute(&format!("EXPORT DATASET 'snap' TO '{}'", dests))
        .unwrap();
    let e = Database::open(&dest).unwrap();
    assert_eq!(one(&e, "SELECT COUNT(*) FROM t"), Value::Integer(200));
    assert_eq!(
        one(&e, "SELECT COUNT(*) FROM t WHERE v = 'v5'"),
        Value::Integer(1)
    );
    assert_eq!(
        one(&e, "SELECT COUNT(*) FROM t WHERE v LIKE 'w%'"),
        Value::Integer(0)
    );
    e.close().unwrap();

    // DROP releases the pin; re-commit works.
    db.execute("DROP DATASET 'snap'").unwrap();
    let ds = rows(&db, "SELECT COUNT(*) AS n FROM _dataset_commits");
    assert_eq!(ds[0][0], Value::Integer(0));
    db.execute("COMMIT DATASET 'snap2'").unwrap();
    assert_ne!(
        one(
            &db,
            "SELECT COUNT(*) FROM _dataset_commits WHERE dataset = 'snap2'"
        ),
        Value::Integer(0)
    );
    // IF EXISTS on a missing name is a no-op.
    db.execute("DROP DATASET IF EXISTS 'nope'").unwrap();
    // Export of an unknown dataset errors.
    assert!(db
        .execute("EXPORT DATASET 'nope' TO '/tmp/definitely-missing-x'")
        .is_err());
    db.close().unwrap();
}
