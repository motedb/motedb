//! Layer-isolation: does concurrency scale on raw row reads vs SQL executes?
use motedb::Database;
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let dir = std::env::temp_dir().join(format!("motedb_layer_{}", std::process::id()));
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

    for threads in [1usize, 4] {
        // layer 1: raw get_row
        let mut hs = Vec::new();
        let t0 = Instant::now();
        for th in 0..threads {
            let db = db.clone();
            hs.push(std::thread::spawn(move || {
                for i in 0..200_000usize {
                    let _ = db.get_row("t", ((th * 7919 + i) % 10_000) as u64);
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        let n = threads as f64 * 200_000.0;
        println!(
            "get_row  {threads}t: {:>9.0} ops/s",
            n / t0.elapsed().as_secs_f64()
        );

        // layer 2: SQL execute point select
        let mut hs = Vec::new();
        let t0 = Instant::now();
        for th in 0..threads {
            let db = db.clone();
            hs.push(std::thread::spawn(move || {
                for i in 0..60_000usize {
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
        let n = threads as f64 * 60_000.0;
        println!(
            "execute  {threads}t: {:>9.0} ops/s",
            n / t0.elapsed().as_secs_f64()
        );
    }
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
