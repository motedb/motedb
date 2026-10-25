//! row_cache 删除幽灵 (round-10)。
//!
//! 缺陷机制: 读路径 cache miss 后回填 (`put_if_absent`) 与删除提交的
//! `invalidate` 交错 — 读者在删除提交前从 store 读到**存活行**, 在删除
//! 提交并 invalidate **之后**才把该行回填进缓存 (此刻条目已不存在,
//! put_if_absent 放行) → 已删除行以"幽灵"形式驻留缓存, 后续按 PK 的
//! SELECT 持续返回它, 直到该 key 再次写入或条目被 LRU 逐出。
//!
//! 判别 (终态确定性, 不依赖窗口运气): 负载结束后 DELETE 全表并确认
//! 提交, 再逐 PK SELECT — 任何一次命中都是幽灵 (store 为权威, 已无行)。
//!
//! 运行: `cargo test --release --test test_row_cache_delete_ghost -- --nocapture`

use motedb::Database;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[test]
fn deleted_rows_never_resurface_via_pk_select() {
    let dir = TempDir::new().unwrap();
    let db = Arc::new(Database::create(dir.path().join("ghost.mote")).unwrap());
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)").unwrap();
    const N: i64 = 50;
    for i in 1..=N {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
            .unwrap();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let mut hs: Vec<std::thread::JoinHandle<()>> = Vec::new();
    // 写者: 循环 [全删 + 全重插] — 制造 invalidate/回填交错。
    {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        hs.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                db.execute("BEGIN").unwrap();
                for i in 1..=N {
                    db.execute(&format!("DELETE FROM t WHERE id = {i}")).unwrap();
                }
                for i in 1..=N {
                    db.execute(&format!("INSERT INTO t VALUES ({i}, {i})"))
                        .unwrap();
                }
                db.execute("COMMIT").unwrap();
            }
        }));
    }
    // 读者 ×4: 按 PK 点查全部行 (命中 row_cache 快路径)。
    for t in 0..4 {
        let (db, stop) = (Arc::clone(&db), Arc::clone(&stop));
        hs.push(std::thread::spawn(move || {
            let mut rng = t as u64 * 17 + 3;
            while !stop.load(Ordering::Relaxed) {
                rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
                let id = (rng >> 16) as i64 % N + 1;
                let _ = db.execute(&format!("SELECT * FROM t WHERE id = {id}"));
            }
        }));
    }
    std::thread::sleep(Duration::from_secs(4));
    stop.store(true, Ordering::Relaxed);
    for h in hs {
        h.join().unwrap();
    }

    // 终态: 全删 (确认提交), 逐 PK SELECT — 必须全部为空。
    db.execute("BEGIN").unwrap();
    for i in 1..=N {
        db.execute(&format!("DELETE FROM t WHERE id = {i}")).unwrap();
    }
    db.execute("COMMIT").unwrap();
    let cnt: i64 = {
        let r = db
            .execute("SELECT COUNT(*) FROM t")
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        match &rows[0][0] {
            motedb::types::Value::Integer(n) => *n,
            _ => 0,
        }
    };
    assert_eq!(cnt, 0, "all rows deleted");
    let mut ghosts = Vec::new();
    for i in 1..=N {
        let r = db
            .execute(&format!("SELECT * FROM t WHERE id = {i}"))
            .unwrap()
            .materialize()
            .unwrap();
        let (_, rows) = r.select_rows().unwrap();
        if !rows.is_empty() {
            ghosts.push(i);
        }
    }
    assert!(
        ghosts.is_empty(),
        "deleted rows served from row_cache (ghosts): {ghosts:?}"
    );
}
