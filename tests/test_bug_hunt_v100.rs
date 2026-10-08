//! Bug Hunt v100 — 外部生产测评反馈 (P1): 全文索引在
//! checkpoint → close → reopen → 新增文档 → close 的生命周期里，
//! flush 报 "[MoteDB] Warning: skipping corrupt page ... during flush"，
//! doctor() 却返回 PASS；实测（本机复现比测评更严重）第三次重开后
//! MATCH 直接抛 `Corruption("Overflow page N not found in page table")`。
//!
//! 根因: overflow 页与普通 B+Tree 页共用 page_offsets 表，重开时
//! `reconstruct_overflow_ids` 用 bytes[13..15] 的 content_len 启发式区分
//! —— overflow 页那两个字节是数据字节，≥16 时被误判成普通页；随后
//! flush() 反序列化失败 → 告警并**直接从重写中丢页** → overflow 链断裂。
//!
//! 修复: (1) 确定性 16 字节 header 分类器（overflow 页 bytes[5..13] 恒
//! ≥ 2^24，普通页 next_leaf 恒为小 id 或 u64::MAX，无歧义）；
//! (2) flush 自愈——header 非普通页且整页符合 overflow 形状的按 overflow
//! 原样重写，永不静默丢页；(3) doctor() 新增 on-disk 索引文件完整性审计
//! (GenericBTree::verify_integrity)，"索引损坏但 doctor PASS" 不再发生。

use motedb::index::btree_generic::{GenericBTree, GenericBTreeConfig};
use motedb::types::Value;
use motedb::Database;
use tempfile::TempDir;

fn count_of(r: &motedb::sql::QueryResult) -> i64 {
    match r {
        motedb::sql::QueryResult::Select { rows, .. } => match rows.first().and_then(|r| r.first())
        {
            Some(Value::Integer(i)) => *i,
            other => panic!("expected single INTEGER count, got {:?}", other),
        },
        other => panic!("expected Select, got {:?}", other),
    }
}

/// 测评方原始场景: 3000 行文本 + TEXT INDEX + checkpoint + close +
/// reopen + 60 行新增 + close + 第三次 reopen。
/// 修复前: close 时大量 corrupt page 告警, 第三开 MATCH 报 Corruption。
#[test]
fn test_fts_reopen_incremental_no_corrupt_page() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("eval.mote");

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE docs(id INT PRIMARY KEY, content TEXT)")
            .unwrap();
        for i in 0..3000 {
            let text = format!(
                "document number {} about robotics sensors and embodied ai perception {}",
                i,
                i % 97
            );
            db.execute(&format!("INSERT INTO docs VALUES ({}, '{}')", i, text))
                .unwrap();
        }
        db.execute("CREATE TEXT INDEX docs_content ON docs(content)")
            .unwrap();
        db.checkpoint().unwrap();
        drop(db);
    }

    {
        let db = Database::open(&path).unwrap();
        for i in 3000..3060 {
            let text = format!(
                "post-reopen document {} with fresh tokens alpha{}",
                i,
                i % 13
            );
            db.execute(&format!("INSERT INTO docs VALUES ({}, '{}')", i, text))
                .unwrap();
        }
        drop(db); // ← 修复前在这里打 corrupt page 告警
    }

    {
        let db = Database::open(&path).unwrap();
        // 数据完整
        assert_eq!(
            count_of(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .unwrap()
                    .materialize()
                    .unwrap()
            ),
            3060
        );
        // 大 posting list ("robotics" 出现在全部前 3000 行 → overflow 链)
        // 修复前这里抛 Corruption("Overflow page N not found in page table")
        assert_eq!(
            count_of(
                &db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(content, 'robotics')")
                    .unwrap()
                    .materialize()
                    .unwrap()
            ),
            3000
        );
        // 新增文档也可检索
        assert!(
            count_of(
                &db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(content, 'alpha5')")
                    .unwrap()
                    .materialize()
                    .unwrap()
            ) > 0
        );
        // 🔑 doctor 必须审计到索引文件且无结构性问题
        let report = db.doctor();
        let integrity = report
            .checks
            .iter()
            .find(|c| c.name == "index.files_integrity")
            .expect("doctor 必须包含 index.files_integrity 检查");
        assert_eq!(
            integrity.detail,
            "1 on-disk index file(s) audited: 0 structural failure(s), 0 with orphans"
        );
        assert!(report.worst() != motedb::database::doctor::DoctorStatus::Fail);
        db.checkpoint().unwrap();
        drop(db);
    }

    // 第四次重开 + 再检索：布局稳定，无累积损坏
    let db = Database::open(&path).unwrap();
    assert_eq!(
        count_of(
            &db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(content, 'robotics')")
                .unwrap()
                .materialize()
                .unwrap()
        ),
        3000
    );
    let report = db.doctor();
    assert_ne!(
        report.worst(),
        motedb::database::doctor::DoctorStatus::Fail,
        "doctor 不应有 FAIL: {:?}",
        report
            .checks
            .iter()
            .filter(|c| c.status == motedb::database::doctor::DoctorStatus::Fail)
            .map(|c| c.detail.clone())
            .collect::<Vec<_>>()
    );
}

/// 多轮 重开-增量-关闭 循环: 布局不能逐轮劣化（旧代码每轮都会丢页）。
#[test]
fn test_fts_reopen_incremental_cycles() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("cycle.mote");

    {
        let db = Database::create(&path).unwrap();
        db.execute("CREATE TABLE docs(id INT PRIMARY KEY, content TEXT)")
            .unwrap();
        for i in 0..1500 {
            let text = format!("base doc {} commonterm tail{:02}", i, i % 7);
            db.execute(&format!("INSERT INTO docs VALUES ({}, '{}')", i, text))
                .unwrap();
        }
        db.execute("CREATE TEXT INDEX docs_content ON docs(content)")
            .unwrap();
        db.checkpoint().unwrap();
        drop(db);
    }

    let mut next = 1500;
    for round in 0..4 {
        let db = Database::open(&path).unwrap();
        for i in next..next + 200 {
            let text = format!("round{round} doc {i} commonterm extra{:02}", i % 11);
            db.execute(&format!("INSERT INTO docs VALUES ({}, '{}')", i, text))
                .unwrap();
        }
        next += 200;
        drop(db);

        let db = Database::open(&path).unwrap();
        assert_eq!(
            count_of(
                &db.execute("SELECT COUNT(*) FROM docs")
                    .unwrap()
                    .materialize()
                    .unwrap()
            ),
            next as i64,
            "round {round}: 行数"
        );
        // commonterm 出现在所有行 → posting list 一定走 overflow 链
        assert_eq!(
            count_of(
                &db.execute("SELECT COUNT(*) FROM docs WHERE MATCH(content, 'commonterm')")
                    .unwrap()
                    .materialize()
                    .unwrap()
            ),
            next as i64,
            "round {round}: 全量 posting list 检索"
        );
        let report = db.doctor();
        assert_ne!(
            report.worst(),
            motedb::database::doctor::DoctorStatus::Fail,
            "round {round}: doctor FAIL: {:?}",
            report
                .checks
                .iter()
                .filter(|c| c.status == motedb::database::doctor::DoctorStatus::Fail)
                .map(|c| c.detail.clone())
                .collect::<Vec<_>>()
        );
        drop(db);
    }
}

/// GenericBTree 单元级: 大值(overflow 链) + flush + 重开循环，值完好；
/// verify_integrity 对人为破坏的页必须报 problems（doctor 依赖这一点）。
#[test]
fn test_btree_overflow_pages_reopen_and_verify() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("ovf.bt");

    // 写入 200 个 5KB 值（> 1024 阈值 → overflow 链）
    {
        let mut bt =
            GenericBTree::<u32>::with_config(path.clone(), GenericBTreeConfig::default()).unwrap();
        let payload: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
        for k in 0..200u32 {
            bt.insert(k, payload.clone()).unwrap();
        }
        bt.flush().unwrap();
    }
    // 重开 + 增量 + flush（评估场景的 B+Tree 投影）
    {
        let mut bt =
            GenericBTree::<u32>::with_config(path.clone(), GenericBTreeConfig::default()).unwrap();
        for k in 200..260u32 {
            bt.insert(k, vec![(k % 255) as u8; 1500]).unwrap();
        }
        bt.flush().unwrap();
        let health = bt.verify_integrity();
        assert!(
            health.problems.is_empty(),
            "健康树不应有 problems: {:?}",
            health.problems
        );
        assert!(health.overflow_pages > 0, "本场景必然产生 overflow 页");
    }
    // 再重开: 全部值可读回
    {
        let bt =
            GenericBTree::<u32>::with_config(path.clone(), GenericBTreeConfig::default()).unwrap();
        let payload: Vec<u8> = (0..5000).map(|i| (i % 251) as u8).collect();
        for k in 0..200u32 {
            let v = bt.get(&k).unwrap().expect("大值丢失");
            assert_eq!(v, payload, "key {k} 的 overflow 值损坏");
        }
        for k in 200..260u32 {
            let v = bt.get(&k).unwrap().expect("增量值丢失");
            assert_eq!(v, vec![(k % 255) as u8; 1500]);
        }
        // 人为破坏（torn-write 形态: 整页 0xFF）→ verify_integrity 必须发现。
        // 注意该格式无 checksum, 只能识别结构性损坏（即旧代码报
        // corrupt page / 丢页的那一类）, 单字节位翻转不在检测范围。
        use std::io::{Seek, SeekFrom, Write};
        let health = bt.verify_integrity();
        assert!(health.problems.is_empty());
        let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        // 覆写 8KB+128 → 必然完整吞掉至少一个页（含 header）
        f.seek(SeekFrom::Start(140_000)).unwrap();
        f.write_all(&[0xFFu8; 8 * 1024 + 128]).unwrap();
        drop(f);
        let broken = bt.verify_integrity();
        assert!(
            !broken.problems.is_empty(),
            "人为整页破坏后 verify_integrity 必须报 problems: {:?}",
            broken.problems
        );
    }
}
