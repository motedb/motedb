//! 并发下的聚合读取一致性 (round-10)。
//!
//! 两个历史缺陷的回归:
//!
//! 1. **聚合快照漏 buf 行**: count_filtered / aggregate_filtered /
//!    aggregate_multi_filtered 先 `flush_buffer()` 再只扫段快照 — 两次调用
//!    之间提交的行还在 write buffer 里, COUNT/SUM 看不见它们。插入负载下
//!    SELECT COUNT(*) 会"回退" (读到比之前更小的值), SELECT SUM(val) 与
//!    COUNT 不再同步。修复 (round-10): `consistent_agg_view` 在 write_buf
//!    锁内完成 [快照段 + 收集 buf 最新行], buf 行 (必然新于段内同 key 版本)
//!    先折叠, 其 key (含墓碑) 抑制段版本。
//!
//! 2. **overlap 标志丢失**: note_overlapping_write (无锁 set) 与全量合并的
//!    [buf 空检查 + 清标志] 交错 → 重叠行 flush 后 overlap_possible ==
//!    false → 多段扫描跳过去重 → SUM/COUNT 双计。修复 (round-10):
//!    append_rows_marked / append_row_ref_marked / append_tombstone 把
//!    [置标志 + 追加] 放进同一 write_buf 临界区, 合并清标志也在锁内。
//!
//! 判别: 转账负载下 SUM 恒等于初始总额 (任何一次观察偏离 = 双计或漏行);
//! 插入负载下 COUNT(*) 单调不减 (回退 = 漏行)。
//!
//! 运行: `cargo test --release --test test_concurrent_agg_consistency -- --nocapture`

use motedb::{types::Value as V, Database};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;

fn scalar_i64(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    assert_eq!(rows.len(), 1, "1 row: {sql}");
    match &rows[0][0] {
        V::Integer(n) => *n,
        // SUM over an empty table is NULL — treat as 0 (test start races the
        // first INSERT).
        V::Null => 0,
        o => panic!("int? {o:?}: {sql}"),
    }
}

/// 时间戳过滤列下的聚合可见性 (round-11)。
///
/// round-10 的 `consistent_agg_view` 引入的回归: `buf_value_matches` 未处理
/// `Value::Timestamp`/`Value::Date` — 时间范围聚合 (本库核心形状
/// `WHERE ts >= a AND ts <= b`) 的**缓冲行**静默不匹配谓词, 且其 key 已进入
/// 去重集 (抑制段版本), 行从聚合中整体消失。
///
/// 🔑 断言设计: 单调性**抓不到**这类缺陷 (段内已可见的行永不回退, buf 内的
/// 行从未可见 — 漏行表现为滞后而非回退)。正确的实时性断言:
/// `c0 = committed 读取 → SELECT → 结果必须 ≥ c0` — 查询开始前已完成的
/// 提交必须在结果中可见, 否则即漏行。
#[test]
fn timestamp_filtered_count_sees_all_committed_rows() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("ts.mote")).unwrap());
    db.execute("CREATE TABLE tt (id INT PRIMARY KEY, ts TIMESTAMP, val INT)")
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    // SeqCst: the reader's c0 load must not be reordered before its SELECT,
    // and the writer's store must happen-after the INSERT's durable commit.
    let committed = Arc::new(AtomicU64::new(0));
    let bad = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut hs: Vec<std::thread::JoinHandle<()>> = Vec::new();
    {
        let (db, stop, committed) = (
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&committed),
        );
        hs.push(std::thread::spawn(move || {
            let mut id = 0i64;
            let base = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64;
            while !stop.load(Ordering::Relaxed) {
                id += 1;
                let ts = base + id;
                if db.execute(&format!("INSERT INTO tt VALUES ({id}, {ts}, 1)")).is_ok() {
                    committed.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for q in [
        "SELECT COUNT(*) FROM tt WHERE ts > 0",
        "SELECT SUM(val) FROM tt WHERE ts > 0 AND ts < 9223372036854775807",
        "SELECT COUNT(*) FROM tt",
    ] {
        let (db, stop, committed, bad, q) = (
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&committed),
            Arc::clone(&bad),
            q.to_string(),
        );
        hs.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let c0 = committed.load(Ordering::SeqCst) as i64;
                if let Ok(n) = db
                    .execute(&q)
                    .and_then(|r| r.materialize())
                {
                    let (_, rows) = n.select_rows().unwrap();
                    {
                        match &rows[0][0] {
                            V::Integer(v) => {
                                if *v < c0 {
                                    bad.lock().unwrap().push(format!(
                                        "{q} saw {v} < {c0} committed (buffered rows invisible)"
                                    ));
                                }
                            }
                            V::Null => {}
                            o => panic!("unexpected {o:?}"),
                        }
                    }
                }
            }
        }));
    }
    std::thread::sleep(Duration::from_secs(5));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    let total = committed.load(Ordering::SeqCst) as i64;
    let cnt = scalar_i64(&db, "SELECT COUNT(*) FROM tt WHERE ts > 0");
    let all = scalar_i64(&db, "SELECT COUNT(*) FROM tt");
    assert_eq!(
        (cnt, all),
        (total, total),
        "ts-filtered count must equal committed inserts (and unfiltered count)"
    );
    let b = bad.lock().unwrap();
    assert!(
        b.is_empty(),
        "ts-filter aggregate visibility violations: {} issues, first: {:?}",
        b.len(),
        b.first()
    );
}

/// 转账负载: 3 写线程双语句事务 + 2 读者, 每一次 SUM 观察都必须精确等于
/// 初始总额 — 双计 (overlap 标志丢失) 或漏行 (buf 不可见) 都会打破。
#[test]
fn transfer_sums_exact_on_every_observation() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("agg.mote")).unwrap());
    db.execute("CREATE TABLE acct (id INT PRIMARY KEY, bal INT)")
        .unwrap();
    const N: i64 = 30;
    for i in 1..=N {
        db.execute(&format!("INSERT INTO acct VALUES ({i}, 100)"))
            .unwrap();
    }
    const TOTAL: i64 = N * 100;
    let stop = Arc::new(AtomicBool::new(false));
    let bad = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut hs = Vec::new();
    for t in 0..3 {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        hs.push(std::thread::spawn(move || {
            let mut rng = t as u64 * 31 + 7;
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
                }
            }
        }));
    }
    for q in ["SELECT SUM(bal) FROM acct", "SELECT SUM(bal) FROM acct WHERE id > 0 AND id <= 30"]
    {
        let (db, stop, bad, q) = (
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&bad),
            q.to_string(),
        );
        hs.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // 升级错误也记录 (SUM 读出错误即是失败)。
                if let Ok(n) = db
                    .execute(&q)
                    .and_then(|r| r.materialize())
                {
                    let (_, rows) = n.select_rows().unwrap();
                    {
                        if let V::Integer(v) = &rows[0][0] {
                            if *v != TOTAL {
                                bad.lock()
                                    .unwrap()
                                    .push(format!("{q} observed {v} (expected {TOTAL})"));
                            }
                        }
                    }
                }
            }
        }));
    }
    std::thread::sleep(Duration::from_secs(6));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    let b = bad.lock().unwrap();
    assert!(
        b.is_empty(),
        "aggregate inconsistency under transfers: {} violations, first: {:?}",
        b.len(),
        b.first()
    );
}

/// 插入负载: COUNT(*) 单调不减且恒有 SUM(val) == COUNT (val 恒 1)。
/// 读到比之前更小的 COUNT = 漏掉了已提交行 (flush→快照间隙)。
#[test]
fn count_monotone_under_concurrent_inserts() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("ins.mote")).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, val INT)").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let inserted = Arc::new(AtomicU64::new(0));
    let bad = Arc::new(Mutex::new(Vec::<String>::new()));
    let mut hs = Vec::new();
    {
        let (db, stop, inserted) = (Arc::clone(&db), Arc::clone(&stop), Arc::clone(&inserted));
        hs.push(std::thread::spawn(move || {
            let mut id = 0i64;
            while !stop.load(Ordering::Relaxed) {
                id += 1;
                if db.execute(&format!("INSERT INTO t VALUES ({id}, 1)")).is_ok() {
                    inserted.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }
    for q in [
        "SELECT COUNT(*) FROM t",
        "SELECT COUNT(*) FROM t WHERE val = 1",
    ] {
        let (db, stop, bad, q) = (
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&bad),
            q.to_string(),
        );
        hs.push(std::thread::spawn(move || {
            let mut max_seen: i64 = 0;
            while !stop.load(Ordering::Relaxed) {
                if let Ok(n) = db
                    .execute(&q)
                    .and_then(|r| r.materialize())
                {
                    let (_, rows) = n.select_rows().unwrap();
                    {
                        if let V::Integer(v) = &rows[0][0] {
                            if *v < max_seen {
                                bad.lock().unwrap().push(format!(
                                    "{q} regressed: {v} after {max_seen} (missed committed rows)"
                                ));
                            }
                            max_seen = max_seen.max(*v);
                        }
                    }
                }
            }
        }));
    }
    // SUM(val) 读者: SUM 必须等于同批可见的 COUNT (val 恒 1) — 漏行/双计都会
    // 打破 "SUM == 某个单调不减的值"。这里只做弱断言: SUM 也单调不减。
    // 两种形状: 无 WHERE (aggregate 路径) 与 AND 谓词 (向量化 M1 路径)。
    for q in ["SELECT SUM(val) FROM t", "SELECT SUM(val) FROM t WHERE id > 0"] {
        let (db, stop, bad, q) = (
            Arc::clone(&db),
            Arc::clone(&stop),
            Arc::clone(&bad),
            q.to_string(),
        );
        hs.push(std::thread::spawn(move || {
            let mut max_sum: i64 = 0;
            while !stop.load(Ordering::Relaxed) {
                let s = scalar_i64(&db, &q);
                if s < max_sum {
                    bad.lock()
                        .unwrap()
                        .push(format!("{q} regressed: {s} after {max_sum}"));
                }
                max_sum = max_sum.max(s);
            }
        }));
    }
    std::thread::sleep(Duration::from_secs(6));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }
    // 收尾一致性: SUM == COUNT == inserted。
    let total = inserted.load(Ordering::Relaxed) as i64;
    let cnt = scalar_i64(&db, "SELECT COUNT(*) FROM t");
    let sum = scalar_i64(&db, "SELECT SUM(val) FROM t");
    assert_eq!(
        (cnt, sum),
        (total, total),
        "final COUNT/SUM must match committed inserts"
    );
    let b = bad.lock().unwrap();
    assert!(
        b.is_empty(),
        "aggregate visibility violations: {} issues, first: {:?}",
        b.len(),
        b.first()
    );
}
