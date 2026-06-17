use motedb::storage::col_segment::ColSegmentStore;
use motedb::types::{ColumnType, Value};
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", vec![ColumnType::Integer, ColumnType::Text, ColumnType::Float, ColumnType::Text]).unwrap();
    let n = 300_000usize;
    let bs = 5000;
    for start in (0..n).step_by(bs) {
        let end = (start + bs).min(n);
        let rows: Vec<(u64, u64, Vec<Value>)> = (start..end).map(|i| {
            (i as u64, i as u64, vec![
                Value::Integer(i as i64),
                Value::Text(format!("cust_{}", i % 30000).into()),
                Value::Float((i as f64 * 1.7) % 1000.0),
                Value::Text(if i % 3 == 0 { "US" } else { "EU" }.to_string().into()),
            ])
        }).collect();
        store.append_rows(&rows).unwrap();
    }
    store.flush_buffer().unwrap();
    while store.needs_compaction() { let _ = store.compact_once(); }

    let t = Instant::now();
    let out = store.scan_projected_filtered(None, &[0,1,2,3], &|_| true);
    eprintln!("full scan 4col: {} rows {}ms", out.len(), t.elapsed().as_millis());

    let t = Instant::now();
    let out = store.scan_projected_filtered(None, &[0,2], &|_| true);
    eprintln!("full scan 2int: {} rows {}ms", out.len(), t.elapsed().as_millis());
    println!("DONE");
}
