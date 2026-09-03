//! Decompose the 51ms write-heavy filter: flush / merge / scan costs.
use motedb::{DBConfig, Database};
use std::time::Instant;
use tempfile::TempDir;

fn main() {
    let dir = TempDir::new().unwrap();
    let mut config = DBConfig::for_general();
    config.auto_checkpoint = None;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY AUTO_INCREMENT, name TEXT, val FLOAT, code TEXT, ts BIGINT)").unwrap();
    let mut total = 0usize;
    while total < 1_000_000 {
        let end = (total + 5000).min(1_000_000);
        let mut sql = String::with_capacity(5000 * 70);
        sql.push_str("INSERT INTO t (name, val, code, ts) VALUES ");
        for i in total..end {
            sql.push_str(&format!(
                "('user_{}', {:.2}, 'CD_{:04}', {})",
                i % 1000,
                (i as f64) * 0.5,
                i % 1000,
                1_700_000_000_i64 + i as i64
            ));
            if i + 1 < end {
                sql.push(',');
            }
        }
        db.execute(&sql).unwrap();
        total = end;
    }
    // Warm: force compaction + preload + col_cache.
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE val > 500");
    for i in 0..2 {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE val > 500");
    }
    let t = Instant::now();
    let _ = db.execute("SELECT COUNT(*) FROM t WHERE val > 500");
    println!("warm filter (no writes between): {:?}\n", t.elapsed());

    // Now simulate the probe loop: insert batch + filter, and time the filter.
    let mut ins = total;
    for round in 0..10 {
        let b = ins;
        ins += 500;
        let mut sql = String::with_capacity(500 * 70);
        sql.push_str("INSERT INTO t (name, val, code, ts) VALUES ");
        for i in b..b + 500 {
            sql.push_str(&format!(
                "('user_{}', {:.2}, 'CD_{:04}', {})",
                i % 1000,
                (i as f64) * 0.5,
                i % 1000,
                1_700_000_000_i64 + i as i64
            ));
            if i + 1 < b + 500 {
                sql.push(',');
            }
        }
        let t = Instant::now();
        db.execute(&sql).unwrap();
        let ins_us = t.elapsed().as_micros();

        let th = (round * 97) % 1000;
        let t = Instant::now();
        let _ = db.execute(&format!("SELECT COUNT(*) FROM t WHERE val > {th}.0"));
        let f1 = t.elapsed();
        let t = Instant::now();
        let _ = db.execute(&format!("SELECT COUNT(*) FROM t WHERE val > {th}.0"));
        println!(
            "round {round}: insert {ins_us}µs, filter#1 {:?}, filter#2 {:?}",
            f1,
            t.elapsed()
        );
    }
}
