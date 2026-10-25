//! ✅ 并发双行转账守恒 (round-9 已修复, 2026-10-23 定位并归零)。
//!
//! 形状: 3 写线程 × 双语句事务 (`UPDATE a-1; UPDATE b+1`) × 2 读线程。
//! 历史 bug: ~330 次成功提交中丢 1-5 个净单位 — 单边转账被静默丢弃,
//! 且 checkpoint+重开后仍丢失 (真持久损坏)。
//!
//! 根因 (round-9, 三层叠加, 每层都由探针实证):
//! 1. **flush 不可见窗口**: `flush_buffer_locked` 旧协议先把 write_buf
//!    整体换出、再写段文件、最后发布段 — I/O 期间行在两个视图都不在,
//!    并发点查返回旧段值。修复: 快照协议 (锁内 snapshot → 锁外写文件 →
//!    锁内 drain+发布原子完成; 点查在 write_buf 锁内完成 [buf 检查 + 段
//!    列表快照])。
//! 2. **row_cache 投毒**: 读路径 cache miss 后用无条件 `put` 回填, 会在
//!    提交者 (write_lock 内 put 新值) 之后把瞬态旧值覆盖回去; 下一个事务
//!    以毒值为基准计算, 而语句水位线晚于冲突提交戳, first-committer-wins
//!    验证无法察觉。修复: 读路径回填全部改 `put_if_absent` (提交者保持
//!    覆盖式 put)。
//! 3. **合并集陈旧快照**: compact_once / force_compact_all /
//!    compact_small_tail 在 flush_merge_lock 之外快照段列表, 快照与拿锁
//!    之间并发 flush 发布的新段不在合并集内; 合并输出 (旧数据) 被 push
//!    到列表尾部, 点查 `.rev()` 优先命中它 → 旧值遮蔽新值, 后续合并的
//!    rank 去重再把被遮蔽的新版本永久丢弃 (持久丢失的直接来源)。
//!    修复: 三个入口全部改为锁内快照; D2 合并的同 key 胜者从纯 rank 改
//!    为 (timestamp, rank), 时间戳来自单调 write_lsn。
//!
//! 运行: `cargo test --release --test test_concurrent_transfer_drift -- --nocapture`
use motedb::{types::Value as V, Database};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
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
        "transfer conservation (round-9 root cause fixed — see module docs)"
    );
}
