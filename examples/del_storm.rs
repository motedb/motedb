use motedb::Database;
fn main() {
    let dir = std::env::temp_dir().join(format!("delstorm_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = Database::create(&dir).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, cat TEXT, val FLOAT, note TEXT, ts TIMESTAMP)")
        .unwrap();
    let mut rng: u64 = 88172645463325252;
    let mut next = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };
    // shuffled inserts like the adversarial harness
    let mut ids: Vec<u64> = (1..=20000).collect();
    for i in (1..ids.len()).rev() {
        let j = (next() as usize) % (i + 1);
        ids.swap(i, j);
    }
    for chunk in ids.chunks(2000) {
        let vals: Vec<String> = chunk
            .iter()
            .map(|i| {
                format!(
                    "({i}, 'cat-{}', 1.5, 'alpha beta', {})",
                    i % 50,
                    1700000000000000 + i
                )
            })
            .collect();
        db.execute(&format!("INSERT INTO t VALUES {}", vals.join(",")))
            .unwrap();
    }
    println!("inserted 20000");
    // 3000 random UPDATEs first — mirroring the adversarial harness
    let t0u = std::time::Instant::now();
    for k in 0..3000u64 {
        let id = (next() % 20000) + 1;
        let v = (next() % 1000) as f64 / 7.0;
        db.execute(&format!("UPDATE t SET val = {v} WHERE id = {id}"))
            .unwrap();
        if k % 1000 == 0 {
            println!("update #{k}: total {:?}", t0u.elapsed());
        }
    }
    println!("3000 updates: {:?}", t0u.elapsed());
    // delete 200 random ids, timing each
    let t0 = std::time::Instant::now();
    for k in 0..200u64 {
        let id = (next() % 20000) + 1;
        let t1 = std::time::Instant::now();
        db.execute(&format!("DELETE FROM t WHERE id = {id}"))
            .unwrap();
        if k % 50 == 0 || k < 5 {
            println!("delete #{k}: {:?} (total {:?})", t1.elapsed(), t0.elapsed());
        }
    }
    println!("200 deletes total: {:?}", t0.elapsed());
    let _ = std::fs::remove_dir_all(&dir);
}
