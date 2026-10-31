//! 事务内聚合的 read-your-writes (round-11 探针)。
//!
//! 扫描路径有 execute_full_scan_txn_merge (事务写合并), 但 COUNT/SUM/
//! AVG 的聚合快路径直接读 ColSegmentStore — 事务自己的缓冲写 (pending
//! updates/deletes、write_set inserts) 若对聚合不可见:
//!   BEGIN; INSERT 3 行; SELECT COUNT(*) → 应为 3 (若读到 0 = 不可见);
//!   BEGIN; DELETE 2 行; SELECT COUNT(*) → 应减 2。
//!
//! 运行: `cargo test --release --test test_txn_read_your_writes -- --nocapture`

use motedb::{types::Value as V, Database};
use tempfile::TempDir;

fn scalar(db: &Database, sql: &str) -> i64 {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rows) = r.select_rows().unwrap();
    match &rows[0][0] {
        V::Integer(n) => *n,
        V::Null => 0,
        o => panic!("int? {o:?}: {sql}"),
    }
}

#[test]
fn aggregate_sees_own_txn_writes() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("ryw.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=10 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, 1)"))
            .unwrap();
    }

    // 1. 事务内 INSERT → COUNT/SUM 必须立刻可见。
    db.execute("BEGIN").unwrap();
    db.execute("INSERT INTO t VALUES (11, 1)").unwrap();
    db.execute("INSERT INTO t VALUES (12, 1)").unwrap();
    let c = scalar(&db, "SELECT COUNT(*) FROM t");
    let s = scalar(&db, "SELECT SUM(v) FROM t");
    db.execute("COMMIT").unwrap();
    assert_eq!(c, 12, "COUNT must see own buffered INSERTs (got {c})");
    assert_eq!(s, 12, "SUM must see own buffered INSERTs (got {s})");

    // 2. 事务内 DELETE → COUNT 必须立刻减少。
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id <= 2").unwrap();
    let c2 = scalar(&db, "SELECT COUNT(*) FROM t");
    let c2f = scalar(&db, "SELECT COUNT(*) FROM t WHERE v = 1");
    db.execute("COMMIT").unwrap();
    assert_eq!(c2, 10, "COUNT must see own buffered DELETEs (got {c2})");
    assert_eq!(
        c2f, 10,
        "filtered COUNT must see own buffered DELETEs (got {c2f})"
    );

    // 3. 事务内 UPDATE → WHERE 过滤的 COUNT 必须反映新值。
    //    (此刻表内容: id 3..12 共 10 行 v=1; UPDATE id>8 → id 9..12 四行
    //    变 v=0 → 剩 id 3..8 六行 v=1。)
    db.execute("BEGIN").unwrap();
    db.execute("UPDATE t SET v = 0 WHERE id > 8").unwrap();
    let c3 = scalar(&db, "SELECT COUNT(*) FROM t WHERE v = 1");
    let s3 = scalar(&db, "SELECT SUM(v) FROM t");
    db.execute("COMMIT").unwrap();
    assert_eq!(
        c3, 6,
        "filtered COUNT must see own buffered UPDATEs (got {c3})"
    );
    assert_eq!(s3, 6, "SUM must see own buffered UPDATEs (got {s3})");

    // 4. ROLLBACK 后聚合必须回到原值。
    //    (此刻表: id 3..12 共 10 行; DELETE id>5 删 7 行 → 剩 id 3..5。)
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id > 5").unwrap();
    let c4 = scalar(&db, "SELECT COUNT(*) FROM t");
    db.execute("ROLLBACK").unwrap();
    let c4b = scalar(&db, "SELECT COUNT(*) FROM t");
    assert_eq!(c4, 3, "in-txn count after delete (got {c4})");
    assert_eq!(c4b, 10, "rollback restores aggregate view (got {c4b})");
}

/// PK 点查的事务 read-your-writes (round-11 探针并入)。
#[test]
fn pk_select_sees_own_txn_delete_and_update() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("p.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=5 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i}00)"))
            .unwrap();
    }
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 3").unwrap();
    let r1 = rows_of(&db, "SELECT v FROM t WHERE id = 3");
    db.execute("UPDATE t SET v = 999 WHERE id = 4").unwrap();
    let r2 = rows_of(&db, "SELECT v FROM t WHERE id = 4");
    db.execute("INSERT INTO t VALUES (10, 7)").unwrap();
    let r3 = rows_of(&db, "SELECT v FROM t WHERE id = 10");
    db.execute("ROLLBACK").unwrap();
    let r1b = rows_of(&db, "SELECT v FROM t WHERE id = 3");
    let r3b = rows_of(&db, "SELECT v FROM t WHERE id = 10");
    assert!(r1.is_empty(), "deleted-in-txn row via PK select: {r1:?}");
    assert_eq!(
        r2,
        vec![vec![V::Integer(999)]],
        "updated-in-txn value via PK select"
    );
    assert_eq!(
        r3,
        vec![vec![V::Integer(7)]],
        "inserted-in-txn row via PK select"
    );
    assert_eq!(
        r1b,
        vec![vec![V::Integer(300)]],
        "rollback restores deleted row"
    );
    assert!(r3b.is_empty(), "rollback removes inserted row");
}

fn rows_of(db: &Database, sql: &str) -> Vec<Vec<V>> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rs) = r.select_rows().unwrap();
    rs.to_vec()
}
