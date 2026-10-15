//! checkpoint 固定成本探针: noop 与脏 1 行的 p50/p90 (可选表数参数)。
//! 用法: cargo run --release --example probe_ckpt [tables]
use motedb::{DBConfig, Database};
use std::time::Instant;

fn main() {
    let n_tables: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let dir = std::env::temp_dir().join(format!("mote_ck_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let path = dir.join("d.mote");
    let db = Database::create_with_config(&path, DBConfig::default()).unwrap();
    for t in 0..n_tables {
        db.execute(&format!(
            "CREATE TABLE t{t} (id INTEGER PRIMARY KEY, v TEXT)"
        ))
        .unwrap();
    }

    // 预热
    for i in 0..5 {
        db.execute(&format!("INSERT INTO t0 VALUES ({i}, 'x')"))
            .unwrap();
        db.checkpoint().unwrap();
    }
    // 空转 checkpoint (无脏数据 — 早退路径)
    let mut t_noop = Vec::new();
    for _ in 0..30 {
        let t = Instant::now();
        db.checkpoint().unwrap();
        t_noop.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    t_noop.sort_by(|a, b| a.partial_cmp(b).unwrap());

    // 每次脏 1 行 (t0)
    let mut t_dirty = Vec::new();
    for i in 100..130 {
        db.execute(&format!("INSERT INTO t0 VALUES ({i}, 'x')"))
            .unwrap();
        let t = Instant::now();
        db.checkpoint().unwrap();
        t_dirty.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    t_dirty.sort_by(|a, b| a.partial_cmp(b).unwrap());

    println!(
        "[{n_tables} tables] noop p50 {:.2}ms | dirty1row-checkpoint p50 {:.2}ms p90 {:.2}ms",
        t_noop[15], t_dirty[15], t_dirty[27]
    );
    db.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
