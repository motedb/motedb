use motedb::types::Value;
use motedb::Database;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;

#[test]
fn deadlock_mix_stress() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 0..50i64 {
        db.execute(&format!("INSERT INTO a VALUES ({i}, {i})"))
            .unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({i}, {i})"))
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let heartbeats = Arc::new(AtomicU64::new(0));
    let (tx, rx) = mpsc::channel::<String>();

    let mut handles = Vec::new();
    // 1) checkpoint loop
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        let tx = tx.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.checkpoint();
                hb.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(120));
            }
            let _ = tx;
        }));
    }
    // 2) backup loop
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let dest = std::env::temp_dir().join(format!(
                    "motedb_dlk_bak_{}_{}",
                    std::process::id(),
                    n % 3
                ));
                let _ = std::fs::remove_dir_all(&dest);
                let _ = db.backup_to(&dest);
                n += 1;
                hb.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(250));
            }
        }));
    }
    // 3) two concurrent INSERT writers (same + different tables)
    for t in 0..2u64 {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let table = if (i + t) % 2 == 0 { "a" } else { "b" };
                let id = 1000 + t * 100_000 + i;
                let _ = db.execute(&format!("INSERT INTO {table} VALUES ({id}, {i})"));
                let _ = db.execute(&format!("UPDATE {table} SET v = v + 1 WHERE id = {id}"));
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 4) UPDATE/DELETE churn
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let id = (i * 7) % 50;
                let _ = db.execute(&format!("UPDATE a SET v = v + 1 WHERE id = {id}"));
                if i % 5 == 4 {
                    let _ = db.execute(&format!("DELETE FROM a WHERE v > 1000000 AND id = {id}"));
                }
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 5) SELECT churn (point + scan)
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = db.execute(&format!("SELECT v FROM a WHERE id = {}", i % 50));
                let _ = db.execute("SELECT COUNT(*) FROM b WHERE v < 25");
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 6) DDL churn
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let name = format!("tmp_{i}");
                let _ = db.execute(&format!("CREATE TABLE {name} (id INT PRIMARY KEY, x INT)"));
                let _ = db.execute(&format!("INSERT INTO {name} VALUES (1, 1)"));
                let _ = db.execute(&format!("DROP TABLE {name}"));
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // watchdog: any progress within 10s windows for 45s total
    let start = Instant::now();
    let mut last_hb = heartbeats.load(Ordering::Relaxed);
    let mut deadlocked = false;
    while start.elapsed() < Duration::from_secs(15) {
        std::thread::sleep(Duration::from_secs(5));
        let cur = heartbeats.load(Ordering::Relaxed);
        eprintln!("t={:?} heartbeats={cur}", start.elapsed());
        if cur == last_hb {
            eprintln!("!!! NO PROGRESS in 5s — likely deadlock");
            deadlocked = true;
            break;
        }
        last_hb = cur;
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    drop(rx);
    assert!(!deadlocked, "deadlock detected under mixed load");
}

#[test]
fn deadlock_mix_stress_hard() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path()).unwrap());
    db.execute("CREATE TABLE a (id INT PRIMARY KEY, v INT, body TEXT)")
        .unwrap();
    db.execute("CREATE TABLE b (id INT PRIMARY KEY, v REAL)")
        .unwrap();
    for i in 0..100i64 {
        db.execute(&format!(
            "INSERT INTO a VALUES ({i}, {i}, 'text {} alpha beta')",
            i % 10
        ))
        .unwrap();
        db.execute(&format!("INSERT INTO b VALUES ({i}, {}.25)", i))
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let heartbeats = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    // 1) VACUUM loop (heaviest maintenance op)
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.vacuum();
                hb.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(400));
            }
        }));
    }
    // 2) checkpoint loop
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.checkpoint();
                hb.fetch_add(1, Ordering::Relaxed);
                std::thread::sleep(Duration::from_millis(100));
            }
        }));
    }
    // 3) explicit txn + savepoint churn
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let tx = db.begin_transaction().unwrap();
                for k in 0..3i64 {
                    let _ = db.insert_row_with_txn(
                        "a",
                        tx,
                        vec![
                            Value::Integer(900_000i64 + (i as i64) * 10 + k),
                            Value::Integer(k),
                            Value::Text(format!("tx {i} {k} gamma").into()),
                        ],
                    );
                }
                let _ = db.savepoint(tx, format!("sp{i}").as_str());
                let _ = db.execute("UPDATE a SET v = v + 1 WHERE v < 3");
                if i % 3 == 0 {
                    let _ = db.rollback_to_savepoint(tx, &format!("sp{i}"));
                }
                let _ = db.commit_transaction(tx);
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 4) prepared DML churn
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            use motedb::types::Value;
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = db.execute_prepared(
                    "UPDATE b SET v = v + ? WHERE id = ?",
                    vec![Value::Float(0.5), Value::Integer((i % 100) as i64)],
                );
                let _ = db.execute_prepared(
                    "SELECT v FROM b WHERE id = ?",
                    vec![Value::Integer((i % 100) as i64)],
                );
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 5) FTS index create/drop + search
    {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let idx = format!("ftx{i}");
                let _ = db.execute(&format!("CREATE TEXT INDEX {idx} ON a(body)"));
                let _ = db.wait_for_indexes_ready();
                let _ = db.text_search_ranked(&idx, "gamma", 5);
                let _ = db.execute(&format!("DROP INDEX {idx}"));
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }
    // 6) concurrent writers on both tables
    for t in 0..2u64 {
        let db = db.clone();
        let stop = stop.clone();
        let hb = heartbeats.clone();
        handles.push(std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let _ = db.execute(&format!(
                    "INSERT INTO b VALUES ({}, 0.5)",
                    500_000u64 + t * 100_000 + i
                ));
                let _ = db.execute(&format!(
                    "DELETE FROM b WHERE id = {}",
                    500_000u64 + t * 100_000 + i.saturating_sub(3)
                ));
                i += 1;
                hb.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    let start = Instant::now();
    let mut last_hb = heartbeats.load(Ordering::Relaxed);
    let mut deadlocked = false;
    while start.elapsed() < Duration::from_secs(20) {
        std::thread::sleep(Duration::from_secs(5));
        let cur = heartbeats.load(Ordering::Relaxed);
        eprintln!("t={:?} heartbeats={cur}", start.elapsed());
        if cur == last_hb {
            eprintln!("!!! NO PROGRESS in 5s — likely deadlock");
            deadlocked = true;
            break;
        }
        last_hb = cur;
    }
    stop.store(true, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }
    assert!(!deadlocked, "deadlock detected under hard mixed load");
}
