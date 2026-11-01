//! `MoteDB::drop_table` (raw API) must drop the table's ColSegmentStore and
//! segment files like SQL `DROP TABLE` does. v0.16.0 only removed the
//! catalog entry + LSM range, so re-creating a same-named table reused the
//! still-registered store and saw the dropped rows again — and recovery
//! re-adopted the old segment directory for the new table.

use motedb::types::{ColumnDef, ColumnType, TableSchema, Value};
use motedb::MoteDB;
use tempfile::TempDir;

fn schema() -> TableSchema {
    TableSchema::new(
        "t".into(),
        vec![
            ColumnDef::new("id".into(), ColumnType::Integer, 0),
            ColumnDef::new("v".into(), ColumnType::Integer, 1),
        ],
    )
}

/// Rows visible through the table's segment store (the SQL read path's
/// source of truth), after draining the write buffer.
fn segment_rows(db: &MoteDB) -> Vec<Vec<Value>> {
    db.checkpoint().unwrap();
    let store = db
        .get_or_create_col_segment_store("t", &[ColumnType::Integer, ColumnType::Integer])
        .unwrap();
    let mut rows: Vec<Vec<Value>> = store.scan().map(|(_, _, row)| row).collect();
    rows.sort_by_key(|r| match r[0] {
        Value::Integer(i) => i,
        _ => i64::MAX,
    });
    rows
}

#[test]
fn raw_api_drop_table_then_recreate_does_not_resurrect_rows() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("d.mote");
    {
        let db = MoteDB::create(&path).unwrap();
        db.create_table(schema()).unwrap();
        for i in 0..5 {
            db.insert_row_to_table("t", vec![Value::Integer(i), Value::Integer(i * 10)])
                .unwrap();
        }
        assert_eq!(segment_rows(&db).len(), 5);

        db.drop_table("t").unwrap();
        db.create_table(schema()).unwrap();
        db.insert_row_to_table("t", vec![Value::Integer(100), Value::Integer(1)])
            .unwrap();

        let rows = segment_rows(&db);
        assert_eq!(rows.len(), 1, "dropped rows resurrected: {rows:?}");
        assert_eq!(rows[0][0], Value::Integer(100));
    }

    let db = MoteDB::open(&path).unwrap();
    let rows = segment_rows(&db);
    assert_eq!(rows.len(), 1, "after reopen: {rows:?}");
    assert_eq!(rows[0][0], Value::Integer(100));
}
