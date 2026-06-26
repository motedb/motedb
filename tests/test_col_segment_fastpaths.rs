//! Direct unit tests for ColSegmentStore fast-path methods that currently have
//! zero callers in production code: top_k_row_indices, distinct_text_values,
//! scan_text_eq_build. These methods are the foundation for future query
//! optimizations (vectorized scans, index-driven DISTINCT, top-K ORDER BY).
//!
//! By testing them directly we lock in their correctness now, so wiring them
//! into the executor later carries no correctness risk.

use motedb::storage::col_segment::ColSegmentStore;
use motedb::types::{ColumnType, Value};
use tempfile::TempDir;

fn make_store(col_types: Vec<ColumnType>) -> (TempDir, std::sync::Arc<ColSegmentStore>) {
    let dir = TempDir::new().unwrap();
    // Use a fixed sub-name so create can be called once.
    let store = ColSegmentStore::create(dir.path(), "t", col_types).unwrap();
    (dir, store)
}

// (key, timestamp, deleted, values) flattened to append_rows format
fn row(key: u64, ts: u64, vals: Vec<Value>) -> (u64, u64, Vec<Value>) {
    (key, ts, vals)
}

// ═══════════════════════════════════════════════════════════════════════════
// top_k_row_indices
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_top_k_desc_returns_largest() {
    let (_dir, store) = make_store(vec![ColumnType::Integer, ColumnType::Float]);
    // col 1 = score. Values 10.0, 50.0, 30.0, 90.0, 20.0
    let rows: Vec<_> = vec![
        row(1, 1, vec![Value::Integer(1), Value::Float(10.0)]),
        row(2, 1, vec![Value::Integer(2), Value::Float(50.0)]),
        row(3, 1, vec![Value::Integer(3), Value::Float(30.0)]),
        row(4, 1, vec![Value::Integer(4), Value::Float(90.0)]),
        row(5, 1, vec![Value::Integer(5), Value::Float(20.0)]),
    ];
    store.append_rows(&rows).unwrap();
    store.flush_buffer().unwrap();
    // Top-2 by col 1 (score) DESC → should be rows with 90.0, 50.0
    let top = store.top_k_row_indices(1, 2, true);
    assert_eq!(top.len(), 2, "top-2 returns 2 indices");
    // The indices reference (segment_idx, local_row). Fetch the scores to verify.
    let segs = store.segments_snapshot();
    let mut scores: Vec<f64> = top.iter().filter_map(|(s, r)| {
        let seg = segs.get(*s)?;
        // col 1 is Float — decode as f64.
        seg.sst.read_fixed_f64(1).ok().and_then(|f| f.get_f64(*r))
    }).collect();
    scores.sort_by(|a, b| b.partial_cmp(a).unwrap());
    assert_eq!(scores, vec![90.0, 50.0], "DESC top-2 must be 90 and 50");
}

#[test]
fn test_top_k_asc_returns_smallest() {
    let (_dir, store) = make_store(vec![ColumnType::Integer, ColumnType::Float]);
    let rows: Vec<_> = vec![
        row(1, 1, vec![Value::Integer(1), Value::Float(10.0)]),
        row(2, 1, vec![Value::Integer(2), Value::Float(50.0)]),
        row(3, 1, vec![Value::Integer(3), Value::Float(5.0)]),
    ];
    store.append_rows(&rows).unwrap();
    store.flush_buffer().unwrap();
    let top = store.top_k_row_indices(1, 2, false);
    let segs = store.segments_snapshot();
    let mut scores: Vec<f64> = top.iter().filter_map(|(s, r)| {
        let seg = segs.get(*s)?;
        seg.sst.read_fixed_f64(1).ok().and_then(|f| f.get_f64(*r))
    }).collect();
    scores.sort_by(|a, b| a.partial_cmp(b).unwrap());
    assert_eq!(scores, vec![5.0, 10.0], "ASC top-2 must be 5 and 10");
}

#[test]
fn test_top_k_k_exceeds_rows() {
    let (_dir, store) = make_store(vec![ColumnType::Integer]);
    store.append_rows(&[row(1, 1, vec![Value::Integer(10)])]).unwrap();
    store.flush_buffer().unwrap();
    let top = store.top_k_row_indices(0, 10, true);
    assert_eq!(top.len(), 1, "k>rows returns all rows");
}

#[test]
fn test_top_k_empty_store() {
    let (_dir, store) = make_store(vec![ColumnType::Integer]);
    store.flush_buffer().unwrap();
    let top = store.top_k_row_indices(0, 5, true);
    assert!(top.is_empty(), "empty store returns no indices");
}

#[test]
fn test_top_k_k_zero() {
    let (_dir, store) = make_store(vec![ColumnType::Integer]);
    store.append_rows(&[row(1, 1, vec![Value::Integer(10)])]).unwrap();
    store.flush_buffer().unwrap();
    let top = store.top_k_row_indices(0, 0, true);
    assert!(top.is_empty(), "k=0 returns nothing");
}

// ═══════════════════════════════════════════════════════════════════════════
// distinct_text_values
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_distinct_text_low_cardinality() {
    let (_dir, store) = make_store(vec![ColumnType::Integer, ColumnType::Text]);
    // 100 rows, only 3 distinct values
    let rows: Vec<_> = (0..100).map(|i| {
        let region = match i % 3 { 0 => "US", 1 => "EU", _ => "AS" };
        row(i + 1, 1, vec![Value::Integer(i as i64), Value::text(region.to_string())])
    }).collect();
    store.append_rows(&rows).unwrap();
    store.flush_buffer().unwrap();
    let vals = store.distinct_text_values(1, 10000);
    let mut s: Vec<String> = vals.clone();
    s.sort();
    assert_eq!(s, vec!["AS".to_string(), "EU".to_string(), "US".to_string()],
              "low-cardinality column returns exactly the distinct values");
}

#[test]
fn test_distinct_text_respects_max_values() {
    let (_dir, store) = make_store(vec![ColumnType::Text]);
    let rows: Vec<_> = (0..50).map(|i| {
        row(i as u64 + 1, 1, vec![Value::text(format!("v{}", i))])
    }).collect();
    store.append_rows(&rows).unwrap();
    store.flush_buffer().unwrap();
    // max_values=5 caps the result
    let vals = store.distinct_text_values(0, 5);
    assert!(vals.len() <= 5, "result capped at max_values");
    assert!(vals.len() >= 1, "returns at least 1");
}

#[test]
fn test_distinct_text_empty_store() {
    let (_dir, store) = make_store(vec![ColumnType::Text]);
    store.flush_buffer().unwrap();
    let vals = store.distinct_text_values(0, 100);
    assert!(vals.is_empty(), "empty store yields no distinct values");
}

// ═══════════════════════════════════════════════════════════════════════════
// scan_text_eq_build (WHERE col = 'val' fast path)
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn test_scan_text_eq_build_matches() {
    let (_dir, store) = make_store(vec![ColumnType::Integer, ColumnType::Text]);
    let rows: Vec<_> = vec![
        row(1, 1, vec![Value::Integer(1), Value::text("US".to_string())]),
        row(2, 1, vec![Value::Integer(2), Value::text("EU".to_string())]),
        row(3, 1, vec![Value::Integer(3), Value::text("US".to_string())]),
        row(4, 1, vec![Value::Integer(4), Value::text("US".to_string())]),
    ];
    store.append_rows(&rows).unwrap();
    store.flush_buffer().unwrap();
    // WHERE col1 = 'US', project [0, 1]
    let result = store.scan_text_eq_build(1, "US", &[0, 1], &[ColumnType::Integer, ColumnType::Text], 100);
    let rows = result.expect("scan returned Some");
    assert_eq!(rows.len(), 3, "3 rows match 'US'");
    for r in &rows {
        assert_eq!(r[1], Value::text("US".to_string()), "all rows must be US");
    }
}

#[test]
fn test_scan_text_eq_build_no_match() {
    let (_dir, store) = make_store(vec![ColumnType::Text]);
    store.append_rows(&[row(1, 1, vec![Value::text("a".to_string())])]).unwrap();
    store.flush_buffer().unwrap();
    let result = store.scan_text_eq_build(0, "zzz", &[0], &[ColumnType::Text], 100);
    let rows = result.expect("scan returned Some");
    assert!(rows.is_empty(), "no match returns empty vec");
}

#[test]
fn test_scan_text_eq_build_limit() {
    let (_dir, store) = make_store(vec![ColumnType::Integer, ColumnType::Text]);
    let rows: Vec<_> = (0..10).map(|i| {
        row(i as u64 + 1, 1, vec![Value::Integer(i), Value::text("dup".to_string())])
    }).collect();
    store.append_rows(&rows).unwrap();
    store.flush_buffer().unwrap();
    let result = store.scan_text_eq_build(1, "dup", &[0, 1], &[ColumnType::Integer, ColumnType::Text], 3);
    let rows = result.expect("scan returned Some");
    assert_eq!(rows.len(), 3, "limit caps result at 3");
}
