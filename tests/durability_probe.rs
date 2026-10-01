//! W2 probe: autocommit single-statement INSERT throughput per durability
//! level — locates whether the ~160 rows/s autocommit figure is fsync-bound
//! (fixable by exposing the durability knob) or per-statement-path-bound.

use motedb::config::{DBConfig, DurabilityLevel};
use motedb::types::Value;
use motedb::Database;
use std::time::Instant;

fn probe(level: DurabilityLevel, name: &str) {
    let dir = tempfile::TempDir::new().unwrap();
    let mut config = DBConfig::for_general();
    config.wal_config.durability_level = level;
    let db = Database::create_with_config(dir.path(), config).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    // warm
    for i in 0..20 {
        db.execute_prepared("INSERT INTO t VALUES (?, ?)", vec![Value::Integer(i), Value::Integer(i)])
            .unwrap();
    }
    let n = 300;
    let t0 = Instant::now();
    for i in 20..20 + n {
        db.execute_prepared("INSERT INTO t VALUES (?, ?)", vec![Value::Integer(i), Value::Integer(i)])
            .unwrap();
    }
    let per_stmt = t0.elapsed() / (n as u32);
    println!("{name:14} {n} autocommit INSERTs: {:?}/stmt  ({:.0} rows/s)",
             per_stmt, n as f64 / t0.elapsed().as_secs_f64());
}

#[test]
fn durability_levels_autocommit_probe() {
    probe(DurabilityLevel::NoSync, "NoSync");
    probe(DurabilityLevel::periodic(100), "Periodic100");
    probe(DurabilityLevel::group_commit(), "GroupCommit");
    probe(DurabilityLevel::Synchronous, "Synchronous");
}
