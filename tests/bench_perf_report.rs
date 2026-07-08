//! Final performance report comparing key operations.
//! Run: cargo test --release --test bench_perf_report -- --nocapture --test-threads=1

use motedb::{DBConfig, Database, DurabilityLevel};
use std::time::Instant;

fn us(d: std::time::Duration) -> u64 {
    d.as_micros() as u64
}

#[test]
#[ignore = "bench/stress/perf: slow in debug, run with --ignored or via bench examples"]
fn perf_report() {
    let n: i64 = 20_000;
    let dir = "/tmp/motedb_final";
    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(format!("{}.mote", dir));
    let mut cfg = DBConfig::default();
    cfg.wal_config.durability_level = DurabilityLevel::no_sync();
    let db = Database::create_with_config(dir, cfg).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, name TEXT, score FLOAT, cat TEXT, amt INT)")
        .unwrap();

    println!("\n╔══════════════════════════════════════════════════════╗");
    println!("║  MoteDB v0.6 Performance Report (N={}, NoSync)      ║", n);
    println!("╚══════════════════════════════════════════════════════╝");

    let t = Instant::now();
    for i in 1..=n {
        let cat = ["A", "B", "C", "D"][(i % 4) as usize];
        db.execute(&format!(
            "INSERT INTO t VALUES ({}, 'n{}', {}, '{}', {})",
            i,
            i,
            i as f64 * 1.5,
            cat,
            i * 7
        ))
        .unwrap();
    }
    let ins_us = us(t.elapsed());
    println!(
        "  INSERT single-row:   {:>8.0} TPS  ({:.1}µs/op)",
        n as f64 * 1e6 / ins_us as f64,
        ins_us as f64 / n as f64
    );

    db.flush().unwrap();
    let m = |label: &str, sql: &str, it: usize| -> f64 {
        let t = Instant::now();
        for _ in 0..it {
            let _ = db.execute(sql);
        }
        let avg = us(t.elapsed()) as f64 / it as f64;
        println!("  {:<24} {:>8.1}µs", label, avg);
        avg
    };

    println!("\n  ── Read Path ──");
    m("COUNT(*)", "SELECT COUNT(*) FROM t", 100);
    m(
        "COUNT WHERE eq",
        "SELECT COUNT(*) FROM t WHERE cat = 'A'",
        50,
    );
    m(
        "COUNT WHERE range",
        "SELECT COUNT(*) FROM t WHERE amt > 50000",
        50,
    );
    m(
        "GROUP BY cat",
        "SELECT cat, COUNT(*) FROM t GROUP BY cat",
        20,
    );
    m(
        "GROUP BY + AVG",
        "SELECT cat, COUNT(*), AVG(score) FROM t GROUP BY cat",
        20,
    );
    m(
        "ORDER BY + LIMIT",
        "SELECT * FROM t ORDER BY score DESC LIMIT 10",
        20,
    );
    m("SELECT * LIMIT 100", "SELECT * FROM t LIMIT 100", 20);

    let t = Instant::now();
    for i in 0..2000 {
        let _ = db.execute(&format!("SELECT * FROM t WHERE id = {}", (i % n) + 1));
    }
    println!(
        "  {:<24} {:>8.1}µs",
        "PK point lookup",
        us(t.elapsed()) as f64 / 2000.0
    );

    println!("\n  ── Write Path ──");
    let t = Instant::now();
    for i in 1..=2000i64 {
        db.execute(&format!(
            "UPDATE t SET score = {} WHERE id = {}",
            i as f64, i
        ))
        .unwrap();
    }
    println!(
        "  {:<24} {:>8.1}µs/op",
        "UPDATE",
        us(t.elapsed()) as f64 / 2000.0
    );

    let t = Instant::now();
    db.execute("CREATE TABLE t2 (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for b in 0..50 {
        let mut sql = String::from("INSERT INTO t2 VALUES ");
        for i in 0..100 {
            if i > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({}, {})", b * 100 + i, i));
        }
        db.execute(&sql).unwrap();
    }
    println!(
        "  {:<24} {:>8.0} TPS",
        "INSERT batch(100)",
        5000.0 * 1e6 / us(t.elapsed()) as f64
    );
}
