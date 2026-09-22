//! 核心插入引擎剖析基准：insert_rows 吞吐画像（标量 + 向量列形状）。
//!
//! 用法: cargo run --release --example bench_insert_engine [n_rows] [batch] [vec]
//!   vec=0 → 标量表; vec=DIM → 含 VECTOR(DIM) 列
use motedb::types::{ArcVec, Value};
use motedb::{Database, DBConfig};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(100_000);
    let batch: usize = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000);
    let dim: usize = std::env::args()
        .nth(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let dir = std::env::temp_dir().join(format!("mote_ins_bench_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let mut config = DBConfig::default();
    config.max_result_rows = None;
    let db = Database::create_with_config(&dir, config.clone()).unwrap();

    if dim == 0 {
        db.execute(
            "CREATE TABLE events (
                id INTEGER PRIMARY KEY AUTO_INCREMENT,
                user_id INTEGER,
                score FLOAT,
                label TEXT
            )",
        )
        .unwrap();
    } else {
        db.execute(&format!(
            "CREATE TABLE t (id INTEGER PRIMARY KEY AUTO_INCREMENT, c TEXT, v FLOAT, emb VECTOR({dim}), ts BIGINT)"
        ))
        .unwrap();
    }

    let t0 = Instant::now();
    let mut inserted = 0usize;
    while inserted < n {
        let m = batch.min(n - inserted);
        let mut rows = Vec::with_capacity(m);
        for i in 0..m {
            let id = inserted + i;
            if dim == 0 {
                rows.push(vec![
                    Value::Integer(id as i64 % 100_000),
                    Value::Integer((id * 31 % 977) as i64),
                    Value::Float((id as f64) * 0.5),
                    Value::Text(format!("label-{}", id % 1000).into()),
                ]);
            } else {
                let floats: Arc<[f32]> = (0..dim).map(|j| (id + j) as f32 * 0.01).collect();
                rows.push(vec![
                    Value::Text(format!("cust-{}", id % 1000).into()),
                    Value::Float((id as f64) * 0.5),
                    Value::Vector(ArcVec(floats)),
                    Value::Integer(1700000000 + id as i64),
                ]);
            }
        }
        let t_b = Instant::now();
        if dim == 0 {
            db.insert_rows("events", rows).unwrap();
        } else {
            db.insert_rows("t", rows).unwrap();
        }
        let d = t_b.elapsed();
        if std::env::var_os("MOTE_TRACE_BATCH").is_some() {
            eprintln!("[batch] {} rows in {:.1}ms", m, d.as_secs_f64() * 1000.0);
        }
        inserted += m;
    }
    let t_insert = t0.elapsed();
    let t1 = Instant::now();
    db.close().unwrap();
    let t_close = t1.elapsed();

    println!(
        "insert {} rows (dim={}) batches of {}: {:.1}ms ({:.0} rows/s) | close: {:.1}ms",
        n,
        dim,
        batch,
        t_insert.as_secs_f64() * 1000.0,
        n as f64 / t_insert.as_secs_f64(),
        t_close.as_secs_f64() * 1000.0
    );

    let db2 = Database::open_with_config(&dir, config).unwrap();
    let sql = if dim == 0 {
        "SELECT COUNT(*) FROM events"
    } else {
        "SELECT COUNT(*) FROM t"
    };
    let r = db2.execute(sql).unwrap().materialize().unwrap();
    if let motedb::sql::QueryResult::Select { rows, .. } = r {
        println!(
            "verify after reopen: {:?}",
            rows.first().and_then(|rw| rw.first().cloned())
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
