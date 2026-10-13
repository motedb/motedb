//! 文件布局收敛 A/B 基准：WAL 分区数 (1 vs 4) 对写入吞吐/关闭/重开的影响。
//!
//! 用法: cargo run --release --example file_layout_bench
//! 输出: 每种分区数下 — 并发多表写入、单表批量写入、close、reopen 耗时 + 文件数。
use motedb::types::Value;
use motedb::{DBConfig, Database};
use std::sync::Arc;
use std::time::Instant;

const PER_THREAD: usize = 40_000;
const BATCH: usize = 4_000;

fn count_files(root: &std::path::Path) -> usize {
    walkdir_files(root).len()
}

fn walkdir_files(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walkdir_files(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

fn run_mode(num_partitions: u8, tag: &str) {
    let dir = std::env::temp_dir().join(format!("mote_flb_{}_{}", std::process::id(), tag));
    let _ = std::fs::remove_dir_all(&dir);
    let sync = std::env::var("FLB_SYNC").unwrap_or_default();
    let mut config = DBConfig {
        num_partitions,
        ..Default::default()
    };
    if sync == "sync" {
        config.wal_config.durability_level = motedb::DurabilityLevel::Synchronous;
    }
    let db = Arc::new(Database::create_with_config(&dir, config.clone()).unwrap());
    for t in 1..=4 {
        db.execute(&format!(
            "CREATE TABLE t{t} (id INTEGER PRIMARY KEY, grp INTEGER, v FLOAT, note TEXT)"
        ))
        .unwrap();
    }

    // A) 4 线程并发，各写各的表（batch insert_rows）
    let t0 = Instant::now();
    let mut handles = Vec::new();
    for t in 1..=4 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut id = 0usize;
            while id < PER_THREAD {
                let m = BATCH.min(PER_THREAD - id);
                let rows: Vec<Vec<Value>> = (0..m)
                    .map(|i| {
                        vec![
                            Value::Integer((id + i) as i64),
                            Value::Integer(((id + i) % 97) as i64),
                            Value::Float((id + i) as f64 * 0.25),
                            Value::Text(format!("n-{}", (id + i) % 997).into()),
                        ]
                    })
                    .collect();
                db.insert_rows(&format!("t{t}"), rows).unwrap();
                id += m;
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let t_multi = t0.elapsed();

    // B) 单表批量（row_id 散列也会跨分区）
    db.execute("CREATE TABLE bulk (id INTEGER PRIMARY KEY, grp INTEGER, v FLOAT, note TEXT)")
        .unwrap();
    let t1 = Instant::now();
    let mut id = 0usize;
    let total = PER_THREAD * 4;
    while id < total {
        let m = BATCH.min(total - id);
        let rows: Vec<Vec<Value>> = (0..m)
            .map(|i| {
                vec![
                    Value::Integer((id + i) as i64),
                    Value::Integer(((id + i) % 97) as i64),
                    Value::Float((id + i) as f64 * 0.25),
                    Value::Text(format!("n-{}", (id + i) % 997).into()),
                ]
            })
            .collect();
        db.insert_rows("bulk", rows).unwrap();
        id += m;
    }
    let t_single = t1.elapsed();

    let t2 = Instant::now();
    db.close().unwrap();
    let t_close = t2.elapsed();

    let files_closed = count_files(&dir);

    let t3 = Instant::now();
    let db2 = Database::open_with_config(&dir, config).unwrap();
    let t_open = t3.elapsed();
    let r = db2
        .execute("SELECT COUNT(*) FROM bulk")
        .unwrap()
        .materialize()
        .unwrap();
    let n = match r {
        motedb::sql::QueryResult::Select { rows, .. } => rows
            .first()
            .and_then(|rw| rw.first().cloned())
            .map(|v| match v {
                Value::Integer(i) => i,
                _ => -1,
            })
            .unwrap_or(-1),
        _ => -1,
    };
    db2.close().unwrap();

    println!(
        "sync={sync} partitions={nump:>1} | concurrent 4x{pt} rows: {tm:.0}ms ({rps:.0} rows/s) | single-table {tot} rows: {ts:.0}ms ({rps2:.0} rows/s) | close {tc:.0}ms | reopen {to:.0}ms (verify {n}) | files after close: {fc}",
        nump = num_partitions, sync = if sync.is_empty() { "group" } else { &sync },
        pt = PER_THREAD,
        tm = t_multi.as_secs_f64() * 1000.0,
        rps = (PER_THREAD * 4) as f64 / t_multi.as_secs_f64(),
        tot = total,
        ts = t_single.as_secs_f64() * 1000.0,
        rps2 = total as f64 / t_single.as_secs_f64(),
        tc = t_close.as_secs_f64() * 1000.0,
        to = t_open.as_secs_f64() * 1000.0,
        fc = files_closed,
    );
    let _ = std::fs::remove_dir_all(&dir);
}

fn main() {
    // 预热一遍（page cache / allocator），避免首轮回调污染对比
    run_mode(1, "warm");
    run_mode(1, "a");
    run_mode(4, "b");
    run_mode(4, "c");
    run_mode(1, "d");
}
