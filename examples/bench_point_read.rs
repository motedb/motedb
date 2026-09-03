//! Point-read concurrency probe: 1 thread vs N threads doing PK lookups.
//! If wall time is identical, something serializes concurrent readers.
use motedb::Database;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let threads: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    let ops: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(50_000);

    for mode in ["single", "multi"] {
        let dir = std::env::temp_dir().join(format!("motedb_pr_{}_{}", std::process::id(), mode));
        let _ = std::fs::remove_dir_all(&dir);
        let db = Arc::new(Database::create(&dir).unwrap());
        db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
            .unwrap();
        for chunk in 0..100 {
            let vals: Vec<String> = (0..100)
                .map(|i| format!("({}, {})", chunk * 100 + i, i))
                .collect();
            db.execute(&format!("INSERT INTO t VALUES {}", vals.join(", ")))
                .unwrap();
        }
        let start = Instant::now();
        if mode == "single" {
            for i in 0..ops {
                let id = (i % 10_000) as i64;
                db.execute(&format!("SELECT v FROM t WHERE id = {id}"))
                    .unwrap();
            }
        } else {
            let mut handles = Vec::new();
            for th in 0..threads {
                let db = db.clone();
                handles.push(std::thread::spawn(move || {
                    for i in 0..ops {
                        let id = ((th * 7919 + i) % 10_000) as i64;
                        db.execute(&format!("SELECT v FROM t WHERE id = {id}"))
                            .unwrap();
                    }
                }));
            }
            for h in handles {
                h.join().unwrap();
            }
        }
        let elapsed = start.elapsed();
        let total = ops * if mode == "multi" { threads } else { 1 };
        println!(
            "{mode:>6}: {total} pk-lookups in {:?} ({:.0} ops/s)",
            elapsed,
            total as f64 / elapsed.as_secs_f64()
        );
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
