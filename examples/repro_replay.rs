fn main() {
    let mode = std::env::args().nth(1).unwrap_or_default();
    let path = "/tmp/repro_replay.mote";
    if mode == "child" {
        let db = motedb::Database::open(std::path::Path::new(path))
            .or_else(|_| motedb::Database::create(std::path::Path::new(path)))
            .unwrap();
        db.execute("CREATE TABLE IF NOT EXISTS crash_dr (id INT PRIMARY KEY, val INT)").unwrap();
        for k in 0..8i64 {
            db.execute(&format!("INSERT INTO crash_dr VALUES ({k}, -1)")).unwrap();
        }
        // 模拟 kill 测试: 少量轮次后立即退出 (不 Drop → 不 checkpoint)。
        let rounds: i64 = std::env::var("ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
        for i in 0..rounds {
            let k = i % 8;
            let u = (k + 1) % 8;
            db.execute("BEGIN").unwrap();
            db.execute(&format!("DELETE FROM crash_dr WHERE id = {k}")).unwrap();
            db.execute(&format!("INSERT INTO crash_dr VALUES ({k}, {i})")).unwrap();
            db.execute(&format!("UPDATE crash_dr SET val = val + 1000 WHERE id = {u}")).unwrap();
            db.execute("COMMIT").unwrap();
        }
        std::process::exit(0);
    }
    let _ = std::fs::remove_dir_all(path);
    // parent: run child with increasing rounds, reopen + count each time.
    for rounds in [0i64, 1, 3, 5, 8, 13, 21] {
        std::process::Command::new(std::env::current_exe().unwrap())
            .arg("child")
            .env("ROUNDS", rounds.to_string())
            .status().unwrap();
        let db = motedb::Database::open(std::path::Path::new(path)).unwrap();
        let r = db.execute("SELECT id, val FROM crash_dr").unwrap().materialize().unwrap();
        let (_, rows) = r.select_rows().unwrap();
        let rows = rows.to_vec();
        let mut keyed: std::collections::BTreeMap<i64, Vec<i64>> = Default::default();
        for row in &rows {
            if let (Some(motedb::types::Value::Integer(id)), Some(motedb::types::Value::Integer(v))) =
                (row.first(), row.get(1))
            {
                keyed.entry(*id).or_default().push(*v);
            }
        }
        let dups: Vec<_> = keyed.iter().filter(|(_, v)| v.len() > 1).collect();
        println!("rounds={rounds} rows={} dup_keys={:?}", rows.len(), dups);
        let _ = std::fs::remove_dir_all(path);
    }
}
