//! §10 BLOB 标量函数缺口清零回归。
//!
//! v0.13 遗留缺口: BLOB 列上的 WHERE 谓词(`LENGTH(payload) > 2`、
//! `payload IS NOT NULL`)在部分执行器路径下把列解成 NULL —— 全部行被
//! 静默过滤。根因两处:
//! - col_segment/merge.rs SegmentCursor: `ColumnTypeTag::Blob` 落进
//!   `ColData::Opaque`,且 `ColData::Text` 无条件 `get_str`(原始字节过
//!   不了 UTF-8 校验 → None → NULL)。MergeCursor 同时驱动 compaction
//!   重写,若不修,压实会把 BLOB 数据永久替换成 NULL。
//! - row_format.rs decode_var: 无 `ColumnType::Blob` 臂,部分解码路径
//!   同样静默丢行。
use motedb::Database;
use tempfile::TempDir;

fn setup() -> (TempDir, Database) {
    let dir = TempDir::new().unwrap();
    let db = Database::create(dir.path().join("b.mote")).unwrap();
    db.execute("CREATE TABLE frames (id INTEGER PRIMARY KEY, payload BLOB)")
        .unwrap();
    db.execute("INSERT INTO frames VALUES (1, X'00fffe0a0b')")
        .unwrap();
    db.execute("INSERT INTO frames VALUES (2, X'0a0b')")
        .unwrap();
    db.execute("INSERT INTO frames VALUES (3, NULL)").unwrap();
    (dir, db)
}

fn ids(db: &Database, sql: &str) -> Vec<i64> {
    let r = db.execute(sql).unwrap().materialize().unwrap();
    let (_cols, rows) = r.select_rows().unwrap();
    rows.iter()
        .map(|r| match &r[0] {
            motedb::types::Value::Integer(i) => *i,
            other => panic!("expected int id, got {:?}", other),
        })
        .collect()
}

#[test]
fn blob_length_in_where() {
    let (_dir, db) = setup();
    assert_eq!(
        ids(&db, "SELECT id FROM frames WHERE LENGTH(payload) > 2"),
        vec![1]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM frames WHERE LENGTH(payload) = 5"),
        vec![1]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM frames WHERE LENGTH(payload) <= 2"),
        vec![2]
    );
    // ORDER BY variant (different executor path than the plain scan).
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM frames WHERE LENGTH(payload) > 2 ORDER BY id DESC"
        ),
        vec![1]
    );
    // Projection of the scalar alongside the filter.
    let r = db
        .execute("SELECT id, LENGTH(payload) FROM frames WHERE LENGTH(payload) > 2 ORDER BY id")
        .unwrap()
        .materialize()
        .unwrap();
    let (_c, rows) = r.select_rows().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][1], motedb::types::Value::Integer(5));
}

#[test]
fn blob_is_not_null_and_null_semantics() {
    let (_dir, db) = setup();
    assert_eq!(
        ids(
            &db,
            "SELECT id FROM frames WHERE payload IS NOT NULL ORDER BY id"
        ),
        vec![1, 2]
    );
    assert_eq!(
        ids(&db, "SELECT id FROM frames WHERE payload IS NULL"),
        vec![3]
    );
    // LENGTH(NULL) → NULL → filtered under three-valued logic.
    assert_eq!(
        ids(&db, "SELECT id FROM frames WHERE LENGTH(payload) > 0"),
        vec![1, 2]
    );
}

#[test]
fn blob_survives_checkpoint_and_reopen() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("b.mote");
    {
        let db = Database::create(&db_path).unwrap();
        db.execute("CREATE TABLE frames (id INTEGER PRIMARY KEY, payload BLOB)")
            .unwrap();
        for i in 1..=50i64 {
            // Distinct lengths so LENGTH predicates have bite.
            let hex: String = (0..i).map(|j| format!("{:02x}", (i + j) % 256)).collect();
            db.execute(&format!("INSERT INTO frames VALUES ({i}, X'{hex}')"))
                .unwrap();
        }
        db.execute("CHECKPOINT").unwrap();
    }
    let db = Database::open(&db_path).unwrap();
    // Before the merge.rs fix, the segment merge path decoded the BLOB column
    // to NULL — these all returned 0 rows (and compaction would have made it
    // permanent).
    let n = db
        .execute("SELECT COUNT(*) FROM frames WHERE LENGTH(payload) >= 40")
        .unwrap()
        .materialize()
        .unwrap()
        .select_rows()
        .unwrap()
        .1[0][0]
        .clone();
    assert_eq!(
        n,
        motedb::types::Value::Integer(11),
        "LENGTH over 40..50 bytes"
    );
    let r = db
        .execute("SELECT LENGTH(payload) FROM frames WHERE id = 50")
        .unwrap()
        .materialize()
        .unwrap();
    let (_c, rows) = r.select_rows().unwrap();
    assert_eq!(
        rows[0][0],
        motedb::types::Value::Integer(50),
        "byte count survives reopen"
    );
    // Force a segment merge (compaction) and verify data still intact —
    // the pre-fix MergeCursor rewrote BLOB columns as NULLs here.
    db.execute("CHECKPOINT").unwrap();
    let n2 = db
        .execute("SELECT COUNT(*) FROM frames WHERE payload IS NOT NULL")
        .unwrap()
        .materialize()
        .unwrap()
        .select_rows()
        .unwrap()
        .1[0][0]
        .clone();
    assert_eq!(
        n2,
        motedb::types::Value::Integer(50),
        "BLOBs survive compaction"
    );
}
