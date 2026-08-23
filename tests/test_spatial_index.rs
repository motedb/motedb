//! Spatial (i-Octree) index end-to-end.
//!
//! Regression for two real bugs:
//!  1. `CREATE INDEX ix ON t (spatial_col)` (untyped) defaulted to a column
//!     index, whose builder read the column with the TEXT reader and
//!     PANICKED (offset underflow) — now re-inferred to Octree.
//!  2. The API-level `create_ioctree_index(name)` created a dead index:
//!     no registry entry → the INSERT-time backfill could never resolve it →
//!     knn returned 0 forever. It now registers via the "{table}_{column}"
//!     convention and backfills existing rows.

use motedb::types::{Point3D, Value};
use motedb::Database;
use tempfile::TempDir;

fn seed(db: &Database) {
    db.execute("CREATE TABLE pts (id INTEGER PRIMARY KEY, g GEOMETRY)")
        .unwrap();
    for i in 0..20i64 {
        let x = i as f32 * 0.05;
        db.execute(&format!(
            "INSERT INTO pts VALUES ({0}, POINT3D({x}, 0.5, 0.0))",
            i
        ))
        .unwrap();
    }
}

#[test]
fn test_spatial_sql_create_and_knn() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed(&db);
    // Untyped CREATE INDEX on a SPATIAL column → Octree (was: panic).
    db.execute("CREATE INDEX pts_g ON pts (g)").unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 3)
        .unwrap_or_else(|e| panic!("knn: {e}"));
    assert_eq!(r.len(), 3);
    // Nearest to origin is row 0 (x=0.0).
    assert_eq!(r[0].0, 0);

    // Incremental insert after CREATE lands in the index.
    db.execute("INSERT INTO pts VALUES (99, POINT3D(0.001, 0.5, 0.0))")
        .unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 2)
        .unwrap();
    assert_eq!(r.len(), 2);
    // The two closest are now 99 (0.001) and 0 (0.0).
    let ids: Vec<u64> = r.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&99) && ids.contains(&0), "ids: {ids:?}");
}

#[test]
fn test_spatial_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().join("t.mote");
    {
        let db = Database::create(&p).unwrap();
        seed(&db);
        db.execute("CREATE INDEX pts_g ON pts (g)").unwrap();
        let _ = db.close();
    }
    let db = Database::open(&p).unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 3)
        .unwrap();
    assert_eq!(r.len(), 3, "spatial index empty after reopen");
    // Incremental inserts keep working after reopen.
    db.execute("INSERT INTO pts VALUES (50, POINT3D(0.002, 0.5, 0.0))")
        .unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 3)
        .unwrap();
    let ids: Vec<u64> = r.iter().map(|(id, _)| *id).collect();
    assert!(ids.contains(&50), "post-reopen insert not indexed: {ids:?}");
}

#[test]
fn test_spatial_api_create_registers_and_backfills() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed(&db);
    // The Rust API creates by name only — "{table}_{column}" convention.
    db.create_ioctree_index("pts_g").unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 3)
        .unwrap_or_else(|e| panic!("knn after API create: {e}"));
    assert_eq!(r.len(), 3, "API-created index must backfill existing rows");

    // INSERT-time backfill resolves through the registry now.
    db.execute("INSERT INTO pts VALUES (77, POINT3D(0.003, 0.5, 0.0))")
        .unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 2)
        .unwrap();
    let ids: Vec<u64> = r.iter().map(|(id, _)| *id).collect();
    assert!(
        ids.contains(&77),
        "insert after API create not indexed: {ids:?}"
    );
}

#[test]
fn test_spatial_delete_removes_point() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path()).unwrap();
    seed(&db);
    db.execute("CREATE INDEX pts_g ON pts (g)").unwrap();
    db.execute("DELETE FROM pts WHERE id = 0").unwrap();
    let r = db
        .ioctree_knn_search("pts_g", &Point3D::new(0.0, 0.5, 0.0), 5)
        .unwrap();
    let ids: Vec<u64> = r.iter().map(|(id, _)| *id).collect();
    assert!(!ids.contains(&0), "deleted point still in index: {ids:?}");
    // Table data agrees.
    let n = db.query("SELECT COUNT(*) FROM pts").unwrap();
    assert_eq!(n[0][0], Value::Integer(19));
}
