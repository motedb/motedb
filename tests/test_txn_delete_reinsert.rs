//! 事务内 DELETE → 重插同主键 (round-10 D 修复)。
//!
//! 缺陷: 缓冲 DELETE 未提交时, 存储行与 pk_lookup 映射仍在 — 重插同 PK
//! 被存储层查重误报 `Duplicate primary key` (标准 SQL 中合法)。更深的连锁:
//! Integer PK 的重插落**同一复合键**, commit apply/WAL/合并的顺序必须让
//! 墓碑先于重插行, 否则 newest-wins 语义下重插行被墓碑吞掉 (提交后行消失
//! / 重开后消失 — 两处持久性破坏)。
//!
//! 修复 (round-10): insert_row_with_txn 的存储层查重对本事务 pending
//! delete 的 PK 放行; commit 的 WAL 记录与存储批次统一为
//! [删除 → 插入 → 更新] 顺序 (ts 同序分配)。
//!
//! 运行: `cargo test --release --test test_txn_delete_reinsert -- --nocapture`

use motedb::{types::Value as V, Database};
use tempfile::TempDir;

fn rows(db: &Database, sql: &str) -> Vec<Vec<V>> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_, rs) = r.select_rows().unwrap();
    rs.to_vec()
}

fn scalar(db: &Database, sql: &str) -> i64 {
    match rows(db, sql)[0][0].clone() {
        V::Integer(n) => n,
        o => panic!("int? {o:?}"),
    }
}

#[test]
fn delete_then_reinsert_same_pk_in_txn() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    for i in 1..=3 {
        db.execute(&format!("INSERT INTO t VALUES ({i}, {i}00)"))
            .unwrap();
    }

    // 删除 + 重插同 PK, 新值不同。
    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 2").unwrap();
    db.execute("INSERT INTO t VALUES (2, 999)").unwrap();
    db.execute("COMMIT").unwrap();

    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM t"),
        3,
        "row count conserved"
    );
    assert_eq!(
        scalar(&db, "SELECT v FROM t WHERE id = 2"),
        999,
        "re-inserted row is live with the NEW value"
    );
    assert_eq!(scalar(&db, "SELECT SUM(v) FROM t"), 100 + 999 + 300);

    // 持久性: checkpoint + 重开后重插行仍存活 (WAL 顺序: 墓碑先于重插)。
    db.execute("CHECKPOINT").unwrap();
    drop(db);
    let db = Database::open(dir.path().join("t.mote")).unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM t"), 3, "reopen: count");
    assert_eq!(
        scalar(&db, "SELECT v FROM t WHERE id = 2"),
        999,
        "reopen: re-inserted row survives (tombstone precedes it in WAL order)"
    );
}

#[test]
fn delete_then_reinsert_then_update_in_txn() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (7, 1)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 7").unwrap();
    db.execute("INSERT INTO t VALUES (7, 2)").unwrap();
    db.execute("UPDATE t SET v = 3 WHERE id = 7").unwrap();
    db.execute("COMMIT").unwrap();

    assert_eq!(
        scalar(&db, "SELECT v FROM t WHERE id = 7"),
        3,
        "chained delete→insert→update"
    );
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM t"), 1);
}

#[test]
fn reinsert_still_rejects_real_duplicates() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (1, 10)").unwrap();
    db.execute("INSERT INTO t VALUES (2, 20)").unwrap();

    // 不删除就重插 → 仍然必须报重复。
    db.execute("BEGIN").unwrap();
    let err = db
        .execute("INSERT INTO t VALUES (1, 11)")
        .err()
        .expect("must fail");
    assert!(
        err.to_string().contains("Duplicate primary key"),
        "real duplicate must still fail: {err}"
    );
    // 删除 id=1 后重插 id=2 (别人没删) → 仍必须报重复。
    db.execute("DELETE FROM t WHERE id = 1").unwrap();
    let err2 = db
        .execute("INSERT INTO t VALUES (2, 21)")
        .err()
        .expect("must fail");
    assert!(
        err2.to_string().contains("Duplicate primary key"),
        "undeleted PK must still fail: {err2}"
    );
    // 被删的 id=1 重插 → OK; 同一事务内再插一次 id=1 → write_set 查重拦截。
    db.execute("INSERT INTO t VALUES (1, 12)").unwrap();
    let err3 = db
        .execute("INSERT INTO t VALUES (1, 13)")
        .err()
        .expect("must fail");
    assert!(
        err3.to_string().contains("Duplicate primary key"),
        "write_set dup check must still fire: {err3}"
    );
    db.execute("COMMIT").unwrap();
    assert_eq!(scalar(&db, "SELECT COUNT(*) FROM t"), 2);
    assert_eq!(scalar(&db, "SELECT v FROM t WHERE id = 1"), 12);
}

#[test]
fn rollback_of_delete_reinsert_restores_original() {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("t.mote")).unwrap();
    db.execute("CREATE TABLE t (id INT PRIMARY KEY, v INT)")
        .unwrap();
    db.execute("INSERT INTO t VALUES (5, 55)").unwrap();

    db.execute("BEGIN").unwrap();
    db.execute("DELETE FROM t WHERE id = 5").unwrap();
    db.execute("INSERT INTO t VALUES (5, 56)").unwrap();
    db.execute("ROLLBACK").unwrap();

    assert_eq!(
        scalar(&db, "SELECT COUNT(*) FROM t"),
        1,
        "rollback conserves the row"
    );
    assert_eq!(
        scalar(&db, "SELECT v FROM t WHERE id = 5"),
        55,
        "rollback restores the ORIGINAL value"
    );
}
