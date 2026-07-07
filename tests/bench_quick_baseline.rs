//! Quick focused benchmark for optimization targeting embedded/edge devices.
//! Measures: INSERT throughput, PK point lookup, full scan, aggregate, range filter.
//! Fast (~15s) — designed for iteration during optimization.

use motedb::Database;
use std::time::Instant;

fn setup(name: &str) -> Database {
    let dir = format!("/tmp/motedb_qb_{}", name);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(format!("{}.mote", &dir));
    Database::create(&dir).unwrap()
}

fn us(elapsed: std::time::Duration) -> u128 {
    elapsed.as_micros()
}

fn fmt(us: u128) -> String {
    if us < 1000 {
        format!("{}µs", us)
    } else {
        format!("{:.2}ms", us as f64 / 1000.0)
    }
}

#[test]
fn bench_quick() {
    let n: usize = 10_000;
    let db = setup("quick");

    // Create table
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT, cat TEXT)")
        .unwrap();

    // === INSERT throughput ===
    let t = Instant::now();
    for i in 1..=n as i64 {
        let cat = ["A", "B", "C", "D"][(i % 4) as usize];
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'item_{}', {}, '{}')",
            i,
            i,
            i as f64 * 1.5,
            cat
        ))
        .unwrap();
    }
    let insert_us = us(t.elapsed());
    let insert_tps = n as f64 / (insert_us as f64 / 1e6);
    println!("\n═══ MoteDB Quick Benchmark (N={}) ═══", n);
    println!(
        "  INSERT single-row:       {:>10}  {:>10.0} TPS",
        fmt(insert_us),
        insert_tps
    );

    db.flush().unwrap();

    // === PK point lookup (warm) ===
    let iters = 500;
    let t = Instant::now();
    for i in 0..iters {
        let id = (i % n as i64) + 1;
        let _ = db.execute(&format!("SELECT * FROM t WHERE id = {}", id));
    }
    let pk_us = us(t.elapsed());
    let pk_avg = pk_us / iters as u128;
    println!(
        "  PK point lookup (x{}):   {:>10}  {:>10}/op",
        iters,
        fmt(pk_us),
        fmt(pk_avg)
    );

    // === Full scan COUNT(*) ===
    let iters2 = 20;
    let t = Instant::now();
    for _ in 0..iters2 {
        let _ = db.execute("SELECT COUNT(*) FROM t");
    }
    let cnt_us = us(t.elapsed()) / iters2 as u128;
    println!(
        "  COUNT(*) full scan:       {:>10}  ({} rows)",
        fmt(cnt_us),
        n
    );

    // === WHERE filter scan ===
    let t = Instant::now();
    for _ in 0..iters2 {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE cat = 'A'");
    }
    let filt_us = us(t.elapsed()) / iters2 as u128;
    println!("  COUNT WHERE filter:       {:>10}", fmt(filt_us));

    // === Range query ===
    let t = Instant::now();
    for _ in 0..iters2 {
        let _ = db.execute("SELECT COUNT(*) FROM t WHERE id > 5000");
    }
    let range_us = us(t.elapsed()) / iters2 as u128;
    println!("  COUNT range (id>5000):    {:>10}", fmt(range_us));

    // === GROUP BY ===
    let t = Instant::now();
    for _ in 0..10 {
        let _ = db.execute("SELECT cat, COUNT(*) FROM t GROUP BY cat");
    }
    let grp_us = us(t.elapsed()) / 10;
    println!("  GROUP BY cat:             {:>10}", fmt(grp_us));

    // === ORDER BY + LIMIT ===
    let t = Instant::now();
    for _ in 0..10 {
        let _ = db.execute("SELECT * FROM t ORDER BY score DESC LIMIT 10");
    }
    let ord_us = us(t.elapsed()) / 10;
    println!("  ORDER BY + LIMIT 10:      {:>10}", fmt(ord_us));

    // === Batch INSERT (100 rows) ===
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    let batch_size = 100;
    let batches = 50;
    let t = Instant::now();
    for b in 0..batches {
        let mut sql = String::from("INSERT INTO t2 VALUES ");
        for i in 0..batch_size {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({}, {})", b * batch_size + i, i));
        }
        db.execute(&sql).unwrap();
    }
    let batch_us = us(t.elapsed());
    let batch_tps = (batch_size * batches) as f64 / (batch_us as f64 / 1e6);
    println!(
        "  INSERT batch(100):        {:>10}  {:>10.0} TPS",
        fmt(batch_us),
        batch_tps
    );

    // === UPDATE ===
    let t = Instant::now();
    for i in 1..=500i64 {
        db.execute(&format!(
            "UPDATE t SET score = {} WHERE id = {}",
            i as f64, i
        ))
        .unwrap();
    }
    let upd_us = us(t.elapsed());
    println!(
        "  UPDATE (x500):            {:>10}  {:>10}/op",
        fmt(upd_us),
        fmt(upd_us / 500)
    );

    println!("═══════════════════════════════════");
}
