//! Measures autocommit write concurrency: 1 thread vs N threads.
//! If wall time is identical, the global write_lock serializes everything.
use motedb::Database;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let dir = std::env::temp_dir().join(format!("motedb_acl_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);

    let threads: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let per_thread: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);

    for (mi, mode) in ["single", "multi"].iter().enumerate() {
        let dir = dir.with_extension(mode);
        let _ = std::fs::remove_dir_all(&dir);
        let dir = std::env::temp_dir().join(format!("motedb_acl_{}_{}", std::process::id(), mi));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Arc::new(Database::create(&dir).unwrap());
        db.execute("CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute("CREATE TABLE t2 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute("CREATE TABLE t3 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();
        db.execute("CREATE TABLE t4 (id INTEGER PRIMARY KEY, v INTEGER)")
            .unwrap();

        let start = Instant::now();
        if *mode == "single" {
            let total = threads * per_thread;
            for i in 0..total {
                let t = i % 4 + 1;
                db.execute(&format!("INSERT INTO t{t} VALUES ({i}, {i})"))
                    .unwrap();
            }
        } else {
            let mut handles = Vec::new();
            for th in 0..threads {
                let db = db.clone();
                let same_table = std::env::var("ACL_SAME_TABLE").is_ok();
                handles.push(std::thread::spawn(move || {
                    for i in 0..per_thread {
                        let t = if same_table { 1 } else { th % 4 + 1 };
                        let id = th * 1_000_000 + i;
                        db.execute(&format!("INSERT INTO t{t} VALUES ({id}, {i})"))
                            .unwrap();
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total_ops = threads * per_thread;
        println!(
            "{mode:>6}: {total_ops} inserts in {:?} ({:.0} ops/s)",
            elapsed,
            total_ops as f64 / elapsed.as_secs_f64()
        );
    }
}
// (appended helper for same-table probe — unused by main)
