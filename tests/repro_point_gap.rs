//! 复现测评报告 "首开 PK 点查 p50 0.093ms vs 重开 0.009ms (10×)" —— 纯 Rust,
//! 不依赖 numpy。与 compete_bench.py 同构: 100K 行加载 → checkpoint →
//! CREATE TEXT INDEX → 首开点查 → 重开点查。
//! 运行: cargo test --release --test repro_point_gap -- --nocapture

use motedb::Database;
use std::time::Instant;

fn p50(mut v: Vec<f64>) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[v.len() / 2]
}

fn bench_point(db: &Database, n: usize) -> f64 {
    // 预热语句缓存
    let _ = db
        .execute_prepared(
            "SELECT val FROM ev WHERE id = ?",
            vec![motedb::types::Value::Integer(0)],
        )
        .unwrap();
    let mut lat = Vec::with_capacity(200);
    let mut x: u64 = 6364136223846793005;
    for _ in 0..200 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let id = (x % n as u64) as i64;
        let t = Instant::now();
        let r = db
            .execute_prepared(
                "SELECT val FROM ev WHERE id = ?",
                vec![motedb::types::Value::Integer(id)],
            )
            .unwrap()
            .materialize()
            .unwrap();
        lat.push(t.elapsed().as_secs_f64() * 1000.0);
        let _ = r;
    }
    p50(lat)
}

#[test]
fn point_query_first_open_vs_reopen() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().join("bench.mote");
    let n = 100_000usize;

    let db = Database::create(&path).unwrap();
    db.execute(
        "CREATE TABLE ev (id INT PRIMARY KEY, ts TIMESTAMP, device TEXT, val FLOAT, note TEXT)",
    )
    .unwrap();
    let t0 = Instant::now();
    for chunk in 0..(n / 5000) {
        let mut rows = Vec::with_capacity(5000);
        for i in 0..5000 {
            let id = (chunk * 5000 + i) as i64;
            rows.push(vec![
                motedb::types::Value::Integer(id),
                motedb::types::Value::Timestamp(motedb::types::Timestamp::from_micros(id * 1000)),
                motedb::types::Value::text(format!("dev-{:02}", id % 16)),
                motedb::types::Value::Float(id as f64 * 0.5),
                motedb::types::Value::text(format!("note {} alpha beta gamma", id)),
            ]);
        }
        db.batch_insert("ev", rows).unwrap();
    }
    println!("load: {:.2}s", t0.elapsed().as_secs_f64());
    db.checkpoint().unwrap();
    let t1 = Instant::now();
    db.execute("CREATE TEXT INDEX ev_note ON ev(note)").unwrap();
    println!("text index: {:.2}s", t1.elapsed().as_secs_f64());

    let first = bench_point(&db, n);
    println!("q_point first-open p50: {:.4} ms", first);
    drop(db);

    let db = Database::open(&path).unwrap();
    let reopened = bench_point(&db, n);
    println!("q_point reopen    p50: {:.4} ms", reopened);
    println!(
        "ratio: {:.1}×",
        if reopened > 0.0 {
            first / reopened
        } else {
            f64::NAN
        }
    );
}
