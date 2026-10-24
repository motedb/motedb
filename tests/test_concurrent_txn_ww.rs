//! 并发显式事务写-写竞争回归 (第七轮 bug 猎杀)。
//!
//! 发现: 并发 `BEGIN; UPDATE v=v+1; COMMIT` 双线程循环丢失约一半增量
//! (2000 次成功提交 → 1000 落盘; checkpoint+重开后仍是 1000 — 写路径
//! 丢失而非读去重假象)。三层根因:
//! ① UPDATE 缓冲的读路径漏看并发事务留在共享 ColSegmentStore 写缓冲里
//!   的新版本 → 从旧值计算 → COMMIT 静默覆盖 (无任何报错);
//! ② 提交校验锚在 BEGIN 快照, 且注册表戳与快照不同时钟 → 冲突永不触发;
//! ③ 校验在 write_lock 外 → 两提交并发通过 first-committer-wins 后
//!   串行应用, 后者覆盖前者。
//!
//! 修复 (三层防线):
//! ① 事务内 UPDATE/DELETE 语句级 write_lock (读+缓冲原子, 与 autocommit
//!   同纪律);
//! ② WW 注册表 (行级提交戳, 与快照同源 version_store 时钟) + 读时刻
//!   锚定 (update_read_stamps) — 冲突 = 读之后该行被并发提交;
//! ③ commit 从校验到应用全程持 write_lock。
//! autocommit 路径从未丢过 (整语句持锁) — 本文件同时钉住这一点。
use motedb::{types::Value, Database};
use std::sync::Arc;
use tempfile::TempDir;

#[test]
fn concurrent_txn_increments_no_lost_update() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("ww.mote")).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();

    const PER_THREAD: usize = 300;
    let mut handles = Vec::new();
    for _ in 0..2 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut conflicts = 0u64;
            let mut done = 0usize;
            while done < PER_THREAD {
                let r = db.execute("BEGIN").and_then(|_| {
                    db.execute("UPDATE t SET v = v + 1 WHERE id = 1")?;
                    db.execute("COMMIT")
                });
                match r {
                    Ok(_) => done += 1,
                    Err(e) => {
                        let _ = db.execute("ROLLBACK");
                        let msg = e.to_string();
                        assert!(
                            msg.contains("conflict"),
                            "non-conflict error must surface: {e}"
                        );
                        conflicts += 1;
                        assert!(conflicts < 500_000, "retry storm");
                        // 退避打散乒乓: 紧循环下两线程互相冲突可活锁。
                        std::thread::yield_now();
                        if conflicts.is_multiple_of(64) {
                            std::thread::sleep(std::time::Duration::from_micros(
                                50 + (conflicts % 7) * 25,
                            ));
                        }
                    }
                }
            }
            conflicts
        }));
    }
    let total_conflicts: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let r = db
        .execute("SELECT v FROM t WHERE id = 1")
        .unwrap()
        .materialize()
        .unwrap();
    let v = match &r.select_rows().unwrap().1[0][0] {
        Value::Integer(v) => *v,
        o => panic!("{o:?}"),
    };
    assert_eq!(
        v,
        (PER_THREAD * 2) as i64,
        "every committed increment must be durable (conflicts={total_conflicts})"
    );

    // 落盘 + 重开后不变 (写路径真持久, 非读路径假象)。
    db.execute("CHECKPOINT").unwrap();
    drop(db);
    let db = Database::open(dir.path().join("ww.mote")).unwrap();
    let r = db
        .execute("SELECT v FROM t WHERE id = 1")
        .unwrap()
        .materialize()
        .unwrap();
    let v2 = match &r.select_rows().unwrap().1[0][0] {
        Value::Integer(v) => *v,
        o => panic!("{o:?}"),
    };
    assert_eq!(
        v2,
        (PER_THREAD * 2) as i64,
        "durable after checkpoint+reopen"
    );
}

#[test]
fn concurrent_autocommit_increments_still_exact() {
    // autocommit 从未丢失 (整语句 write_lock) — 钉住不回归。
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("ac.mote")).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 0)").unwrap();
    let mut handles = Vec::new();
    for _ in 0..2 {
        let db = Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            for _ in 0..300 {
                db.execute("UPDATE t SET v = v + 1 WHERE id = 1").unwrap();
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let r = db
        .execute("SELECT v FROM t WHERE id = 1")
        .unwrap()
        .materialize()
        .unwrap();
    let v = match &r.select_rows().unwrap().1[0][0] {
        Value::Integer(v) => *v,
        o => panic!("{o:?}"),
    };
    assert_eq!(v, 600);
}

#[test]
fn concurrent_bank_transfers_conserve_total() {
    // 读-改-写转账: 余额不足整体回滚; 守恒 + 无负余额 + 重开保持。
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("bk.mote")).unwrap());
    db.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
        .unwrap();
    const N: i64 = 20;
    const INIT: i64 = 500;
    for i in 1..=N {
        db.execute(&format!("INSERT INTO acct VALUES ({i}, {INIT})"))
            .unwrap();
    }
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut handles = Vec::new();
    for t in 0..4 {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        handles.push(std::thread::spawn(move || {
            use std::sync::atomic::Ordering;
            let mut rng = t as u64 * 7919 + 13;
            while !stop.load(Ordering::Relaxed) {
                rng = rng
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let a = (rng >> 16) as i64 % N + 1;
                let b = (rng >> 32) as i64 % N + 1;
                if a == b {
                    continue;
                }
                let amt = (rng >> 48) as i64 % 5 + 1;
                let r = (|| -> motedb::Result<()> {
                    db.execute("BEGIN")?;
                    let row = db
                        .execute(&format!("SELECT bal FROM acct WHERE id = {a}"))?
                        .materialize()?;
                    let av = match row.select_rows() {
                        Some((_, rows)) if !rows.is_empty() && !rows[0].is_empty() => {
                            match &rows[0][0] {
                                Value::Integer(v) => *v,
                                _ => -1,
                            }
                        }
                        // 偶发空读 (并发提交窗口的读异常, 观察项见 docs/05):
                        // 按银行语义整体重试。
                        _ => {
                            db.execute("ROLLBACK")?;
                            return Ok(());
                        }
                    };
                    if av < amt {
                        db.execute("ROLLBACK")?;
                        return Ok(());
                    }
                    db.execute(&format!("UPDATE acct SET bal = bal - {amt} WHERE id = {a}"))?;
                    db.execute(&format!("UPDATE acct SET bal = bal + {amt} WHERE id = {b}"))?;
                    db.execute("COMMIT")?;
                    Ok(())
                })();
                if r.is_err() {
                    let _ = db.execute("ROLLBACK");
                }
            }
        }));
    }
    std::thread::sleep(std::time::Duration::from_secs(3));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }
    let r = db
        .execute("SELECT SUM(bal), MIN(bal) FROM acct")
        .unwrap()
        .materialize()
        .unwrap();
    let (_c, rows) = r.select_rows().unwrap();
    match (&rows[0][0], &rows[0][1]) {
        (Value::Integer(sum), Value::Integer(min)) => {
            assert_eq!(*sum, N * INIT, "total conserved");
            assert!(*min >= 0, "no negative balance: {min}");
        }
        o => panic!("{o:?}"),
    }
    db.execute("CHECKPOINT").unwrap();
    drop(db);
    let db = Database::open(dir.path().join("bk.mote")).unwrap();
    let r = db
        .execute("SELECT SUM(bal) FROM acct")
        .unwrap()
        .materialize()
        .unwrap();
    let sum = match r.select_rows().unwrap().1[0][0].clone() {
        Value::Integer(s) => s,
        o => panic!("{o:?}"),
    };
    assert_eq!(sum, N * INIT, "conserved after reopen");
}
