//! 🔴 已知未修复问题 (2026-10-24 round-8 定位): 并发双行转账漂移。
//!
//! 形状: 3 写线程 × 双语句事务 (`UPDATE a-1; UPDATE b+1`) × 2 读线程,
//! ~330 次成功提交中丢 1-3 个净单位 — 单边转账被静默丢弃。
//! 判别证据 (tests 里复现器同款探针):
//! - agg-SUM == row-SUM == 同一错值, 且 checkpoint+重开后仍同值
//!   → 真持久丢失 (非读路径双计);
//! - 单行形状 (ww7/`concurrent_txn_increments_no_lost_update`) 精确零丢失;
//! - 同类 "SUM 瞬间 ×2/×3" 的读路径双计已由 round-8 的快照后复查修复
//!   (may_have_duplicate_keys 竞态), 本残留是写路径单边丢失。
//! 已修相关项 (本文件同期):
//! - WAL 数据记录先于 Commit 标记 (auto-checkpoint 回放窗口)
//! - M1/store 聚合去重复查
//!
//! 运行: `cargo test --release --test test_concurrent_transfer_drift -- --ignored --nocapture`
//! (默认 ignore: 已知失败, 发版前必须归零)。
use motedb::{types::Value as V, Database};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
#[ignore = "KNOWN-FAIL: concurrent 2-row transfer drift ~1-3 per 330 txns (see module docs)"]
fn concurrent_two_row_transfers_drift() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("td.mote")).unwrap());
    db.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
        .unwrap();
    const N: i64 = 30;
    for i in 1..=N {
        db.execute(&format!("INSERT INTO acct VALUES ({i}, 100)"))
            .unwrap();
    }
    let stop = Arc::new(AtomicBool::new(false));
    let mut hs = Vec::new();
    for t in 0..3 {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        hs.push(std::thread::spawn(move || {
            let mut rng = t as u64 * 31 + 7;
            let mut ok = 0u64;
            while !stop.load(Ordering::Relaxed) {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let a = (rng >> 16) as i64 % N + 1;
                let b = (rng >> 32) as i64 % N + 1;
                if a == b {
                    continue;
                }
                let r = (|| -> motedb::Result<()> {
                    db.execute("BEGIN")?;
                    db.execute(&format!("UPDATE acct SET bal = bal - 1 WHERE id = {a}"))?;
                    db.execute(&format!("UPDATE acct SET bal = bal + 1 WHERE id = {b}"))?;
                    db.execute("COMMIT")?;
                    Ok(())
                })();
                if r.is_err() {
                    let _ = db.execute("ROLLBACK");
                } else {
                    ok += 1;
                }
            }
            ok
        }));
    }
    for _ in 0..2 {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        hs.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.execute("SELECT SUM(bal) FROM acct");
            }
            0u64
        }));
    }
    std::thread::sleep(Duration::from_secs(8));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    let sum = {
        let r = db
            .execute("SELECT SUM(bal) FROM acct")
            .unwrap()
            .materialize()
            .unwrap();
        match &r.select_rows().unwrap().1[0][0] {
            V::Integer(s) => *s,
            o => panic!("{o:?}"),
        }
    };
    db.execute("CHECKPOINT").unwrap();
    drop(db);
    let db = Database::open(dir.path().join("td.mote")).unwrap();
    let sum2 = {
        let r = db
            .execute("SELECT SUM(bal) FROM acct")
            .unwrap()
            .materialize()
            .unwrap();
        match &r.select_rows().unwrap().1[0][0] {
            V::Integer(s) => *s,
            o => panic!("{o:?}"),
        }
    };
    assert_eq!(
        (sum, sum2),
        (N * 100, N * 100),
        "transfer conservation (known-fail: see module docs)"
    );
}
