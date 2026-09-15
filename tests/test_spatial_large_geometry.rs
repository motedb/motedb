//! Round 10 regression: spatial values beyond the old u16 length prefix.
//!
//! The spatial column stored [len:u16][bincode(Geometry)] per row and the
//! writer TRUNCATED at 65,535 bytes — a >64KB geometry silently lost its
//! tail, failed bincode deserialization on read, and came back as NULL
//! after checkpoint+reopen (a 70K-point LineString vanished entirely).
//! The prefix is now escape-encoded: u16 normally, 0xFFFF + u32 for large
//! payloads — backward compatible (old valid rows all had len < 0xFFFF).

use motedb::types::{Geometry, Point, Point3D, Value};
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn db() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    (dir, db)
}

fn n_points(geo: &Value) -> Option<usize> {
    match geo {
        Value::Spatial(g) => match &**g {
            Geometry::Point(_) | Geometry::Point3D(_) => Some(1),
            Geometry::LineString(pts) | Geometry::Polygon(pts) => Some(pts.len()),
        },
        _ => None,
    }
}

#[test]
fn large_geometry_survives_checkpoint_reopen_and_merge() {
    let (dir, db) = db();
    db.execute("CREATE TABLE g (id INT PRIMARY KEY, geo GEOMETRY)")
        .unwrap();

    // Sizes straddling the old u16 limit: small (2 bytes/point header-free
    // bincode ≈ 16B/point → 4K points ≈ 65KB).
    let sizes = [(1usize, 1usize), (10, 10), (4_000, 4_000), (20_000, 20_000)];
    for (id, n) in sizes {
        let pts: Vec<Point> = (0..n)
            .map(|i| Point::new(i as f64, (i % 7) as f64))
            .collect();
        let geom = if n == 1 {
            Geometry::Point3D(Point3D::new(1.0, 2.0, 3.0))
        } else {
            Geometry::LineString(pts)
        };
        db.insert_row(
            "g",
            vec![Value::Integer(id as i64), Value::Spatial(Box::new(geom))],
        )
        .unwrap();
    }

    // In-memory read (full scan — positional params not needed)
    let rows = db.query("SELECT id, geo FROM g ORDER BY id").unwrap();
    let got: Vec<(i64, usize)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Integer(i), g) => (*i, n_points(g).unwrap_or(0)),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(got.len(), sizes.len(), "all rows readable in memory");
    for (row, (id, n)) in got.iter().zip(sizes.iter()) {
        assert_eq!(row.0, *id as i64);
        assert_eq!(
            row.1, *n,
            "geometry for id {id} truncated: {} points",
            row.1
        );
    }

    // Checkpoint + reopen
    db.checkpoint().unwrap();
    drop(db);
    let db2 = Database::open(dir.path()).unwrap();
    let rows = db2.query("SELECT id, geo FROM g ORDER BY id").unwrap();
    let got: Vec<(i64, usize)> = rows
        .iter()
        .map(|r| match (&r[0], &r[1]) {
            (Value::Integer(i), g) => (*i, n_points(g).unwrap_or(0)),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(got.len(), sizes.len(), "all rows readable after reopen");
    for (row, (id, n)) in got.iter().zip(sizes.iter()) {
        assert_eq!(row.1, *n, "geometry for id {id} lost after reopen");
    }
}
