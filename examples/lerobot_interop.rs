//! LeRobot v3 interop demo — export an EPISODE table to a LeRobot dataset
//! directory, for the official loader to read.
//!
//! ```bash
//! cargo run --example lerobot_interop -- /tmp/demo.mote /tmp/out_dataset
//! # then, from Python:
//! #   from lerobot.datasets.lerobot_dataset import LeRobotDataset
//! #   ds = LeRobotDataset("demo", root="/tmp/out_dataset")
//! #   print(len(ds), ds[0]["observation.state"])
//! ```
use motedb::types::{ArcString, ArcVec, Timestamp, Value};
use motedb::Database;

fn main() -> motedb::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("usage: lerobot_interop <db-path> <output-dir>");
        std::process::exit(2);
    }
    let (db_path, out) = (&args[1], &args[2]);
    let db = Database::create(db_path)?;
    db.execute(
        "CREATE EPISODE TABLE teleop (\
         session TEXT, ts TIMESTAMP, joint VECTOR(4), gripper FLOAT, note TEXT) \
         EPISODE BY (session, ts) GAP '5s'",
    )?;
    let base = 1_767_225_600_000_000i64; // 2026-01-01T00:00:00Z
    for ep in 0..2i64 {
        for i in 0..12i64 {
            db.execute_prepared(
                "INSERT INTO teleop VALUES (?, ?, ?, ?, ?)",
                vec![
                    Value::Text(ArcString::from("robot-1")),
                    Value::Timestamp(Timestamp::from_micros(base + ep * 60_000_000 + i * 50_000)),
                    Value::Vector(ArcVec::new(vec![
                        i as f32 + ep as f32 * 100.0,
                        1.0,
                        -1.0,
                        0.5,
                    ])),
                    Value::Float((i % 2) as f64),
                    Value::Text(ArcString::from(format!("demo task {ep}").as_str())),
                ],
            )?;
        }
    }
    db.checkpoint()?;
    let result = db
        .execute(&format!(
            "EXPORT LEROBOT teleop TO '{out}' FPS 20 TASKS note"
        ))?
        .materialize()?;
    println!("{result:?}");
    Ok(())
}
