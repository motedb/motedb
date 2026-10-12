//! ASOF JOIN (v0.14 W8): robotics frame↔joint-state alignment. For each
//! left row, the latest right row per key with right_ts ≤ left_ts.
use motedb::{types::Value, Database};
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

fn seed(db: &Database) {
    // frames: 4 frames at 0, 10, 20, 30s for cam0 (+1 orphan frame for cam1)
    // joints: states at 5s (q=1), 15s (q=2), 25s (q=3) for cam0 — none for cam1
    let t = 1_700_000_000_000_000i64;
    db.execute("CREATE TABLE frames (fname TEXT, session TEXT, ts TIMESTAMP)")
        .unwrap();
    db.execute("CREATE TABLE joints (session TEXT, ts TIMESTAMP, q INTEGER)")
        .unwrap();
    for i in 0..4i64 {
        db.execute(&format!(
            "INSERT INTO frames VALUES ('f{}', 'cam0', {})",
            i,
            t + i * 10_000_000
        ))
        .unwrap();
    }
    db.execute(&format!(
        "INSERT INTO frames VALUES ('orphan', 'cam1', {})",
        t
    ))
    .unwrap();
    for (i, q) in [(0i64, 1i64), (1, 2), (2, 3)] {
        db.execute(&format!(
            "INSERT INTO joints VALUES ('cam0', {}, {})",
            t + (5 + i * 10) * 1_000_000,
            q
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
}

#[test]
fn asof_inner_alignment() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("a.mote")).unwrap();
    seed(&db);

    let out = rows(
        &db,
        "SELECT f.fname, j.q FROM frames f ASOF JOIN joints j ON f.session = j.session AND j.ts <= f.ts ORDER BY f.fname",
    );
    // f0 (ts=0s): latest joint ≤ 0s → none → dropped (INNER).
    // f1 (10s): latest ≤ 10s → 5s (q=1). f2 (20s) → 15s (q=2). f3 (30s) → 25s (q=3).
    // orphan (cam1): no joint rows at all → dropped.
    assert_eq!(out.len(), 3, "{out:?}");
    assert_eq!(out[0][0], Value::text("f1".to_string()));
    assert_eq!(out[0][1], Value::Integer(1));
    assert_eq!(out[1][0], Value::text("f2".to_string()));
    assert_eq!(out[1][1], Value::Integer(2));
    assert_eq!(out[2][0], Value::text("f3".to_string()));
    assert_eq!(out[2][1], Value::Integer(3));
}

#[test]
fn asof_left_keeps_unmatched() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("a.mote")).unwrap();
    seed(&db);

    let out = rows(
        &db,
        "SELECT f.fname, j.q FROM frames f LEFT ASOF JOIN joints j ON f.session = j.session AND j.ts <= f.ts ORDER BY f.fname",
    );
    assert_eq!(out.len(), 5, "{out:?}");
    // f0 + orphan kept with NULL q.
    let null_q = out.iter().filter(|r| matches!(r[1], Value::Null)).count();
    assert_eq!(null_q, 2, "{out:?}");
    assert_eq!(out[0][0], Value::text("f0".to_string()));
}

#[test]
fn asof_reversed_ts_form() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("a.mote")).unwrap();
    seed(&db);
    // Equivalent form: left_ts >= right_ts.
    let out = rows(
        &db,
        "SELECT f.fname, j.q FROM frames f ASOF JOIN joints j ON f.session = j.session AND f.ts >= j.ts ORDER BY f.fname",
    );
    assert_eq!(out.len(), 3, "{out:?}");
    assert_eq!(out[2][1], Value::Integer(3));

    // Wrong direction (right_ts >= left_ts) → clear error.
    assert!(db
        .execute(
            "SELECT f.fname FROM frames f ASOF JOIN joints j ON f.session = j.session AND j.ts >= f.ts"
        )
        .is_err());
    // Residual predicates unsupported v1.
    assert!(db
        .execute(
            "SELECT f.fname FROM frames f ASOF JOIN joints j ON f.session = j.session AND j.ts <= f.ts AND f.fname = 'f1'"
        )
        .is_err());
}

#[test]
fn asof_matches_brute_reference() {
    // Differential: 30 frames × 40 joint states with jittered timestamps —
    // engine ASOF vs brute reference computed in the test.
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("b.mote")).unwrap();
    let t = 1_700_000_000_000_000i64;
    db.execute("CREATE TABLE fr (fid INTEGER PRIMARY KEY, ts TIMESTAMP)")
        .unwrap();
    db.execute("CREATE TABLE jt (jid INTEGER PRIMARY KEY, ts TIMESTAMP, q INTEGER)")
        .unwrap();
    let mut frame_ts = Vec::new();
    for i in 0..30i64 {
        let ts = t + i * 700_000 + (i * 37) % 300_000; // jittered
        frame_ts.push(ts);
        db.execute(&format!("INSERT INTO fr VALUES ({}, {})", i, ts))
            .unwrap();
    }
    let mut joint_ts = Vec::new();
    for i in 0..40i64 {
        let ts = t + i * 500_000 + (i * 53) % 200_000;
        joint_ts.push(ts);
        db.execute(&format!("INSERT INTO jt VALUES ({}, {}, {})", i, ts, i % 7))
            .unwrap();
    }
    db.checkpoint().unwrap();

    let out = rows(
        &db,
        "SELECT fr.fid, jt.q FROM fr ASOF JOIN jt ON fr.ts >= jt.ts ORDER BY fr.fid",
    );
    // Brute reference.
    let mut want = Vec::new();
    for (fid, &fts) in frame_ts.iter().enumerate() {
        let best = joint_ts
            .iter()
            .enumerate()
            .filter(|(_, &jts)| jts <= fts)
            .max_by_key(|(_, &jts)| jts);
        if let Some((jid, _)) = best {
            want.push((
                Value::Integer(fid as i64),
                Some(Value::Integer(jid as i64 % 7)),
            ));
        }
    }
    assert_eq!(out.len(), want.len(), "{out:?}");
    for (row, (fid, q)) in out.iter().zip(&want) {
        assert_eq!(&row[0], fid);
        match q {
            Some(qv) => assert_eq!(&row[1], qv, "fid {:?}", fid),
            None => panic!("expected match for fid {fid:?}"),
        }
    }
    // The first joint state is at t exactly — a frame at t matches it
    // (<= boundary), so the inner result may include fid 0.
    assert_eq!(one(&db, "SELECT MIN(ts) FROM jt"), Value::Integer(t));
}
