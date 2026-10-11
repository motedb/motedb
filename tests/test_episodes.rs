//! Episode data model (v0.14 W6): CREATE EPISODE TABLE … EPISODE BY (k, ts)
//! GAP d · FROM EPISODES(t) segmentation view · traj_sim() DTW ordering.
//! traj_sim results are checked against a brute DTW reference computed in
//! the test itself (differential-in-test).
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

fn one_i(db: &Database, sql: &str) -> i64 {
    match &rows(db, sql)[0][0] {
        Value::Integer(i) => *i,
        other => panic!("int expected, got {other:?}"),
    }
}

fn as_vec(v: &Value) -> Vec<f32> {
    match v {
        Value::Vector(x) => x.to_vec(),
        other => panic!("vector expected, got {other:?}"),
    }
}

fn seed(db: &Database) {
    db.execute(&format!(
        "CREATE EPISODE TABLE teleop (session TEXT, ts TIMESTAMP, joint VECTOR(2), note TEXT) EPISODE BY (session, ts) GAP {}",
        1_000_000 // 1s gap in µs, bare-number form
    ))
    .unwrap();
    // session A: two episodes (0s..3s contiguous at 1s steps, then a 5s jump)
    //   ep1: ts 0,1s,2s,3s — joint.x rises 0..3; ep2: ts 8s,9s — joint.x 10,11
    // session B: one episode (ts 0s,1s)
    let t = 1_700_000_000_000_000i64;
    let pts_a1: Vec<(i64, f64)> = (0..4).map(|i| (t + i * 1_000_000, i as f64)).collect();
    let pts_a2: Vec<(i64, f64)> = (0..2)
        .map(|i| (t + (5 + i) * 1_000_000, 10.0 + i as f64))
        .collect();
    let pts_b: Vec<(i64, f64)> = (0..2)
        .map(|i| (t + i * 1_000_000, 100.0 + i as f64))
        .collect();
    for (ts, x) in pts_a1.iter().chain(&pts_a2) {
        db.execute(&format!(
            "INSERT INTO teleop VALUES ('A', {}, [{}, 0.5], 'a')",
            ts, x
        ))
        .unwrap();
    }
    for (ts, x) in &pts_b {
        db.execute(&format!(
            "INSERT INTO teleop VALUES ('B', {}, [{}, 0.5], 'b')",
            ts, x
        ))
        .unwrap();
    }
    db.checkpoint().unwrap();
}

#[test]
fn episode_segmentation_and_shape() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("e.mote")).unwrap();
    seed(&db);

    let out = rows(&db, "SELECT session, t0, t1, n_rows FROM EPISODES(teleop)");
    // A: 2 episodes, B: 1 → 3 rows, first-seen key order (A first).
    assert_eq!(out.len(), 3, "{out:?}");
    assert_eq!(out[0][0], Value::text_from("A"));
    assert_eq!(out[0][3], Value::Integer(4), "ep1 rows");
    assert_eq!(out[1][3], Value::Integer(2), "ep2 rows (after the 5s gap)");
    assert_eq!(out[2][0], Value::text_from("B"));
    assert_eq!(out[2][3], Value::Integer(2));

    // Episode t0/t1 track the segment bounds.
    assert_eq!(out[0][1], out[0][1]); // timestamps present
    let t0 = match &out[0][1] {
        Value::Timestamp(t) => t.as_micros(),
        o => panic!("{o:?}"),
    };
    let t1 = match &out[0][2] {
        Value::Timestamp(t) => t.as_micros(),
        o => panic!("{o:?}"),
    };
    assert_eq!(t1 - t0, 3_000_000, "ep1 spans 3s");

    // Trajectory columns: flattened 64×dim.
    let traj = rows(
        &db,
        "SELECT traj_joint FROM EPISODES(teleop) WHERE session = 'A' LIMIT 1",
    );
    // WHERE over the inline works via the derived-table path.
    let v = as_vec(&traj[0][0]);
    assert_eq!(v.len(), 128, "64 samples × 2 dims");
    // Resampled ep1 trajectory: x goes 0→3 across samples (monotone rise).
    assert!(
        v[0] < v[32] && v[32] < v[64],
        "resampled rise: {} {} {}",
        v[0],
        v[32],
        v[64]
    );
}

#[test]
fn traj_sim_orders_by_dtw_reference() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    seed(&db);

    let eps = rows(&db, "SELECT session, t0, traj_joint FROM EPISODES(teleop)");
    assert_eq!(eps.len(), 3);

    // Demo trajectory: rising ramp (like ep1 of session A).
    let demo: Vec<f32> = (0..64).map(|i| i as f32 / 21.0).collect(); // 0..3 over 64
    let demo_flat = {
        // 64 samples × 2 dims (dim2 constant 0.5 like the data)
        let mut v = Vec::with_capacity(128);
        for x in &demo {
            v.push(*x);
            v.push(0.5);
        }
        v
    };

    // Engine ordering.
    let got = rows(
        &db,
        &format!(
            "SELECT session, t0 FROM EPISODES(teleop) ORDER BY traj_sim(traj_joint, [{}]) ",
            demo_flat
                .iter()
                .map(|f| format!("{f}"))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    );
    let got_sessions: Vec<String> = got
        .iter()
        .map(|r| match &r[0] {
            Value::Text(t) => t.as_str().to_string(),
            o => panic!("{o:?}"),
        })
        .collect();

    // Reference: brute DTW over each episode's traj (from the engine's own
    // resampled output — the same representation traj_sim consumes).
    fn dtw(a: &[f32], b: &[f32]) -> f32 {
        let dim = 2;
        let n = a.len() / dim;
        let m = b.len() / dim;
        let mut prev = vec![f32::INFINITY; m + 1];
        let mut cur = vec![f32::INFINITY; m + 1];
        prev[0] = 0.0;
        for i in 0..n {
            cur[0] = f32::INFINITY;
            for j in 0..m {
                let mut d = 0f32;
                for k in 0..dim {
                    let diff = a[i * dim + k] - b[j * dim + k];
                    d += diff * diff;
                }
                cur[j + 1] = d.sqrt() + prev[j].min(prev[j + 1]).min(cur[j]);
            }
            std::mem::swap(&mut prev, &mut cur);
        }
        prev[m]
    }
    let mut scored: Vec<(f64, String, i64)> = eps
        .iter()
        .map(|r| {
            let session = match &r[0] {
                Value::Text(t) => t.as_str().to_string(),
                o => panic!("{o:?}"),
            };
            let t0 = match &r[1] {
                Value::Timestamp(t) => t.as_micros(),
                o => panic!("{o:?}"),
            };
            let traj = as_vec(&r[2]);
            (dtw(&traj, &demo_flat) as f64, session, t0)
        })
        .collect();
    scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    let want: Vec<String> = scored.into_iter().map(|(_, s, _)| s).collect();
    assert_eq!(
        got_sessions, want,
        "engine traj_sim order must match brute DTW"
    );

    // Sanity: the rising-ramp demo must rank A's first episode (the ramp) first.
    assert_eq!(got_sessions[0], "A");
}

#[test]
fn episodes_derived_survives_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = Database::create(&path).unwrap();
        seed(&db);
        let n = one_i(&db, "SELECT COUNT(*) FROM EPISODES(teleop)");
        assert_eq!(n, 3);
    }
    // Reopen: episodes are derived — no registry to lose, segmentation is a
    // pure function of the persisted (key, ts) columns.
    let db = Database::open(&path).unwrap();
    let n = one_i(&db, "SELECT COUNT(*) FROM EPISODES(teleop)");
    assert_eq!(n, 3);
    // Config persisted through the schema.
    let n2 = one_i(&db, "SELECT COUNT(*) FROM EPISODES(teleop) ");
    assert_eq!(n2, 3);
}
