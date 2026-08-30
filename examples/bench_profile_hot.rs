//! Round-17 workload profile (kept as a regression benchmark).
//! Reference numbers (M-series, release): insert 500K ≈ 4.7s,
//! filtered scan ≈ 36ms/query, GROUP BY ≈ 30ms/query, point update
//! ≈ 3.5ms (fsync-bound).
use motedb::Database;
use std::time::Instant;

fn main() {
    let dir = std::env::temp_dir().join(format!("motedb_p17_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Database::create(&dir).unwrap();
    db.execute("CREATE TABLE events (id INT PRIMARY KEY, user_id INT, kind INT, val REAL)")
        .unwrap();

    let t0 = Instant::now();
    for c in 0..500 {
        let vals: Vec<String> = (0..1000)
            .map(|i| {
                format!(
                    "({}, {}, {}, {:.2})",
                    c * 1000 + i,
                    (i * 31 + c) % 50000,
                    i % 7,
                    (i as f64) * 0.5
                )
            })
            .collect();
        db.execute(&format!("INSERT INTO events VALUES {}", vals.join(", ")))
            .unwrap();
    }
    println!("insert_500K_batch: {:?}", t0.elapsed());

    let t1 = Instant::now();
    for _ in 0..30 {
        let _ = db
            .execute(
                "SELECT user_id, COUNT(*), AVG(val) FROM events WHERE kind = 3 GROUP BY user_id",
            )
            .unwrap();
    }
    println!(
        "groupby_x30: {:?} ({:.1}ms/q)",
        t1.elapsed(),
        t1.elapsed().as_secs_f64() * 1000.0 / 30.0
    );

    let t2 = Instant::now();
    for _ in 0..30 {
        let _ = db
            .execute("SELECT * FROM events WHERE user_id = 12345 AND kind = 2")
            .unwrap();
    }
    println!(
        "filter_x30: {:?} ({:.1}ms/q)",
        t2.elapsed(),
        t2.elapsed().as_secs_f64() * 1000.0 / 30.0
    );

    let t3 = Instant::now();
    for i in 0..3000 {
        db.execute(&format!(
            "UPDATE events SET val = val + 1 WHERE id = {}",
            (i * 7) % 500_000
        ))
        .unwrap();
    }
    println!(
        "point_update_x3000: {:?} ({:.2}ms/q)",
        t3.elapsed(),
        t3.elapsed().as_secs_f64() * 1000.0 / 3000.0
    );

    // reopen time (clean close path)
    drop(db);
    let t4 = Instant::now();
    let db = Database::open(&dir).unwrap();
    println!("clean_reopen: {:?}", t4.elapsed());
    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
