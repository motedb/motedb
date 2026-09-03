//! Concurrent execute-only scaling probe (for sampling).
use motedb::Database;
use std::sync::Arc;
use std::time::Instant;
fn main() {
    let dir = std::env::temp_dir().join(format!("motedb_eo_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Arc::new(Database::create(&dir).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for c in 0..100 {
        let vals: Vec<String> = (0..100)
            .map(|i| format!("({}, {})", c * 100 + i, i))
            .collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(", ")))
            .unwrap();
    }
    db.checkpoint().unwrap();
    let threads: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8);
    let t0 = Instant::now();
    let mut hs = Vec::new();
    for th in 0..threads {
        let db = db.clone();
        hs.push(std::thread::spawn(move || {
            for i in 0..3_000_000usize {
                let _ = db.execute(&format!(
                    "SELECT v FROM t WHERE id = {}",
                    (th * 7919 + i) % 10_000
                ));
            }
        }));
    }
    for h in hs {
        h.join().unwrap();
    }
    println!(
        "execute-only {threads}t: {:.0} ops/s",
        (threads as f64 * 3_000_000.0) / t0.elapsed().as_secs_f64()
    );
}
