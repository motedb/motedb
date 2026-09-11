//! Spatial SQL semantics found broken by the first accuracy eval
//! (bench/spatial_eval.py, 20K-point synthetic LiDAR scan):
//!
//!  * POINT3D / POINT / ST_* rejected negative coordinates (parser matched
//!    only positive literals; 2D ST_WITHIN/ST_KNN shared the bug);
//!  * `SELECT id, ST_DISTANCE_3D(pt, …)` projected NULL (the evaluator
//!    errored, the projection swallowed it as NULL), so an un-indexed
//!    `ORDER BY ST_DISTANCE_3D … LIMIT k` sorted on NULL and returned
//!    arbitrary rows;
//!  * indexed ORDER BY / KNN fast paths returned SQUARED distances and
//!    appended an extra `distance` column the SELECT list never asked for;
//!  * `COUNT(*) WHERE ST_KNN_3D(…)` returned 0 (a positional aggregate path
//!    treated the unsupported predicate as "no match");
//!  * `WHERE ST_KNN_3D(…)` without an index errored/returned nothing;
//!  * moving many rows to one coordinate silently dropped them from the
//!    index once the min-extent leaf slot filled (32 points).
use motedb::types::Value;
use motedb::{DBConfig, Database};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<Value>> {
    let r = db
        .execute(sql)
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .materialize()
        .unwrap();
    let (_, rows) = r.select_rows().unwrap();
    rows.to_vec()
}

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    rows(db, sql)
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("{sql}: expected Integer, got {other:?}"),
        })
        .collect()
}

fn one(db: &Database, sql: &str) -> Value {
    rows(db, sql)
        .pop()
        .unwrap_or_else(|| panic!("{sql}: no rows"))
        .remove(0)
}

fn db() -> (Database, TempDir) {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, pt GEOMETRY)")
        .unwrap();
    db.execute(
        "INSERT INTO c VALUES \
         (1, POINT3D(1.0, 2.0, 3.0)), (2, POINT3D(-1.0, 2.0, 3.0)), \
         (3, POINT3D(-1.5, 2.0, 0.0)), (4, POINT3D(0.0, 0.0, 0.0))",
    )
    .unwrap();
    (db, dir)
}

/// Distances from (-1, 0, 0): id4 1.0 < id3 ≈2.0616 < id2 ≈3.6056 < id1 ≈4.1231.
const TRUTH_ORDER: [i64; 4] = [4, 3, 2, 1];

#[test]
fn negative_coordinates_parse_everywhere() {
    let (db, _d) = db();
    // Insert (done in db()) + queries with negative coordinates at every site.
    // Nearest to (-1,0,0) is id4 at distance 1.0.
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM c WHERE ST_RADIUS_3D(pt, -1.0, 0.0, 0.0, 0.6)"
        ),
        Vec::<i64>::new()
    );
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM c WHERE ST_RADIUS_3D(pt, -1.0, 0.0, 0.0, 1.1)"
        ),
        [4]
    );
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 1)"
        ),
        [4]
    );
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM c WHERE ST_WITHIN_3D(pt, -2.0, -1.0, -1.0, 0.5, 5.0, 5.0)"
        ),
        [2, 3, 4]
    );
    let d = one(
        &db,
        "SELECT ST_DISTANCE_3D(pt, -1.0, 0.0, 0.0) AS d FROM c WHERE id = 4",
    );
    match d {
        Value::Float(f) => assert!((f - 1.0).abs() < 1e-9),
        other => panic!("distance {other:?}"),
    }
    // 2D sugar with negatives.
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM c WHERE ST_WITHIN(pt, -2.0, 0.0, 0.5, 5.0)"
        ),
        [2, 3, 4]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM c WHERE ST_KNN(pt, -1.5, 2.0, 1)"),
        [3]
    );
}

#[test]
fn order_by_distance_works_with_and_without_index() {
    for indexed in [false, true] {
        let (db, _d) = db();
        if indexed {
            db.execute("CREATE OCTREE INDEX c_pt ON c(pt)").unwrap();
        }
        let got = ids(
            &db,
            "SELECT id FROM c ORDER BY ST_DISTANCE_3D(pt, -1.0, 0.0, 0.0) LIMIT 3",
        );
        assert_eq!(got, TRUTH_ORDER[..3], "indexed={indexed}");
        // The alias form and the SELECT-list value must agree (the value used
        // to come back NULL, or squared, depending on the path).
        let r = rows(
            &db,
            "SELECT id, ST_DISTANCE_3D(pt, -1.0, 0.0, 0.0) AS d FROM c \
             ORDER BY d LIMIT 3",
        );
        let mut prev = -0.1;
        for row in &r {
            match &row[1] {
                Value::Float(f) => {
                    assert!(
                        *f > prev && *f < 5.0,
                        "indexed={indexed} d={f} (squared? NULL?)"
                    );
                    prev = *f;
                }
                other => panic!("indexed={indexed}: distance {other:?}"),
            }
        }
    }
}

#[test]
fn knn_and_radius_project_exactly_the_select_list() {
    let (db, _d) = db();
    db.execute("CREATE OCTREE INDEX c_pt ON c(pt)").unwrap();
    // One column requested → one column returned; the fast path used to
    // append a `distance` column (holding the SQUARED distance).
    let r = db
        .execute("SELECT id FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 3)")
        .unwrap()
        .materialize()
        .unwrap();
    let (cols, rows) = r.select_rows().unwrap();
    assert_eq!(cols.len(), 1, "columns: {cols:?}");
    assert!(rows.iter().all(|r| r.len() == 1));
    let got = rows
        .iter()
        .map(|r| match &r[0] {
            Value::Integer(i) => *i,
            other => panic!("{other:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(got, TRUTH_ORDER[..3]);

    let got = ids(
        &db,
        "SELECT id FROM c WHERE ST_RADIUS_3D(pt, -1.0, 0.0, 0.0, 2.5)",
    );
    // Radius results come back nearest-first.
    assert_eq!(got, [4, 3]);
}

#[test]
fn aggregates_over_spatial_predicates() {
    let (db, _d) = db();
    // Without an index: per-row evaluation.
    assert_eq!(
        one(
            &db,
            "SELECT COUNT(*) FROM c WHERE ST_RADIUS_3D(pt, -1.0, 0.0, 0.0, 2.5)"
        ),
        Value::Integer(2)
    );
    assert_eq!(
        one(
            &db,
            "SELECT SUM(id) FROM c WHERE ST_DISTANCE_3D(pt, -1.0, 0.0, 0.0) < 3"
        ),
        Value::Integer(7)
    );
    // ST_KNN_3D without an index: exact fallback over the geometry column
    // (used to be 0 rows / an error).
    assert_eq!(
        one(
            &db,
            "SELECT COUNT(*) FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 2)"
        ),
        Value::Integer(2)
    );

    db.execute("CREATE OCTREE INDEX c_pt ON c(pt)").unwrap();
    assert_eq!(
        one(
            &db,
            "SELECT COUNT(*) FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 3)"
        ),
        Value::Integer(3)
    );
    assert_eq!(
        one(
            &db,
            "SELECT SUM(id), MAX(id) FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 2)"
        ),
        Value::Integer(7)
    );
    // Compound predicate.
    assert_eq!(
        one(
            &db,
            "SELECT COUNT(*) FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 3) AND id > 2"
        ),
        Value::Integer(2)
    );
    // ORDER BY over a KNN predicate.
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM c WHERE ST_KNN_3D(pt, -1.0, 0.0, 0.0, 3) ORDER BY id DESC"
        ),
        [4, 3, 2]
    );
}

#[test]
fn moving_many_rows_to_one_point_keeps_them_all() {
    let dir = TempDir::new().unwrap();
    let db = Database::create_with_config(dir.path(), DBConfig::for_testing()).unwrap();
    db.execute("CREATE TABLE c (id INT PRIMARY KEY, pt GEOMETRY)")
        .unwrap();
    let vals: Vec<String> = (1..=100)
        .map(|i| format!("({i}, POINT3D({i}.0, 2.0, 3.0))"))
        .collect();
    db.execute(&format!("INSERT INTO c VALUES {}", vals.join(",")))
        .unwrap();
    db.execute("CREATE OCTREE INDEX c_pt ON c(pt)").unwrap();
    for i in 1..=100 {
        db.execute(&format!(
            "UPDATE c SET pt = POINT3D(50.0, 50.0, 50.0) WHERE id = {i}"
        ))
        .unwrap();
    }
    let found = rows(
        &db,
        "SELECT id FROM c WHERE ST_RADIUS_3D(pt, 50.0, 50.0, 50.0, 0.001)",
    )
    .len();
    assert_eq!(found, 100, "coincident-point overflow lost rows");

    // The move must survive a reopen (v3 persistence carries the overflow)
    // and deletes must remove overflow points.
    db.close().unwrap();
    let db = Database::open(dir.path()).unwrap();
    let found = rows(
        &db,
        "SELECT id FROM c WHERE ST_RADIUS_3D(pt, 50.0, 50.0, 50.0, 0.001)",
    )
    .len();
    assert_eq!(found, 100, "overflow lost across reopen");
    for i in (1..=100).step_by(2) {
        db.execute(&format!("DELETE FROM c WHERE id = {i}"))
            .unwrap();
    }
    let found = rows(
        &db,
        "SELECT id FROM c WHERE ST_RADIUS_3D(pt, 50.0, 50.0, 50.0, 0.001)",
    )
    .len();
    assert_eq!(found, 50, "overflow deletes");
}
