//! Isolate scan_projected_filtered cost breakdown.
use motedb::storage::col_segment::ColSegmentStore;
use motedb::types::{ColumnType, Value};
use tempfile::TempDir;
use std::time::Instant;

fn main() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", vec![ColumnType::Integer, ColumnType::Text, ColumnType::Float, ColumnType::Text]).unwrap();
    let n = 300_000usize;
    let batch = 5000;
    for start in (0..n).step_by(batch) {
        let end = (start + batch).min(n);
        let rows: Vec<(u64, u64, Vec<Value>)> = (start..end).map(|i| {
            let region = if i % 3 == 0 { "US" } else { "EU" };
            (i as u64, i as u64, vec![
                Value::Integer(i as i64),
                Value::Text(format!("cust_{}", i % 30000).into()),
                Value::Float((i as f64 * 1.7) % 1000.0),
                Value::Text(region.to_string().into()),
            ])
        }).collect();
        store.append_rows(&rows).unwrap();
    }
    store.flush_buffer().unwrap();
    while store.needs_compaction() { let _ = store.compact_once(); }
    eprintln!("loaded {} rows, segs={}", n, store.segment_count());

    // 1. Pure column read (baseline)
    let t = Instant::now();
    let segs = store.segments_snapshot();
    let mut cnt = 0u64;
    for seg in &segs {
        if let Ok(f) = seg.sst.read_fixed_i64(0) {
            for i in 0..seg.sst.num_rows { if !seg.sst.row_map.is_deleted(i) { let _ = f.get_i64(i); cnt += 1; } }
        }
    }
    eprintln!("[1] pure col read: {} rows in {}ms", cnt, t.elapsed().as_millis());

    // 2. scan_projected_filtered (full scan, no filter)
    let t = Instant::now();
    let out = store.scan_projected_filtered(None, &[0,1,2,3], &|_| true);
    eprintln!("[2] scan_projected (full): {} rows in {}ms", out.len(), t.elapsed().as_millis());

    // 3. scan_projected_filtered (WHERE region='US')
    let t = Instant::now();
    let out = store.scan_projected_filtered(Some(3), &[0,1,2,3], &|fv| fv == Some(&Value::Text("US".into())));
    eprintln!("[3] scan_projected (WHERE=US): {} rows in {}ms", out.len(), t.elapsed().as_millis());

    // 4. scan_projected_filtered (LIKE 'cust_1%')
    let t = Instant::now();
    let out = store.scan_projected_filtered(Some(1), &[0,1,2,3], &|fv| {
        match fv { Some(Value::Text(s)) => s.as_str().starts_with("cust_1"), _ => false }
    });
    eprintln!("[4] scan_projected (LIKE): {} rows in {}ms", out.len(), t.elapsed().as_millis());

    // 5. Without sort (measure sort cost)
    let t = Instant::now();
    let segs2 = store.segments_snapshot();
    let mut cnt2 = 0u64;
    for seg in &segs2 {
        let nr = seg.sst.num_rows;
        let mut order: Vec<usize> = (0..nr).collect();
        order.sort_by_key(|&i| seg.sst.row_map.key(i));
        cnt2 += nr as u64;
    }
    eprintln!("[5] sort_only: {} rows in {}ms", cnt2, t.elapsed().as_millis());

    // 6. Integer-only projection (no Text allocation)
    let t = Instant::now();
    let out = store.scan_projected_filtered(None, &[0,2], &|_| true);
    eprintln!("[6] scan_projected (int-only): {} rows in {}ms", out.len(), t.elapsed().as_millis());

    // 7. Single column projection
    let t = Instant::now();
    let out = store.scan_projected_filtered(None, &[0], &|_| true);
    eprintln!("[7] scan_projected (1 col): {} rows in {}ms", out.len(), t.elapsed().as_millis());

    println!("DONE");
}

fn extra() {
}
