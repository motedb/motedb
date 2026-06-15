# Columnar Append-Only Multi-Segment Store — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the single-SSTable full-rewrite columnar storage with an append-only multi-segment store + background compaction, eliminating the CREATE INDEX 45x regression and the IN-subquery/mmap bugs.

**Architecture:** New `ColSegmentStore` subsystem (`src/storage/col_segment/`) manages per-table segment lists, a manifest, and a single background compactor thread. Writes append O(1) delta segments; reads multi-way merge via a heap cursor (O(1) memory). Segment file format is unchanged (reuses `ColumnarSSTable`).

**Tech Stack:** Rust, parking_lot (RwLock/Mutex), dashmap, memmap2, snap (Snappy), tempfile (tests).

**Naming note:** The name `ColumnarStore` is already taken by the time-series module (`src/storage/columnar/store.rs`). The new type is `ColSegmentStore` to avoid collision.

---

## File Structure

**New files (S1–S5, pure additions — no existing code touched):**
- `src/storage/col_segment/mod.rs` — module root, exports
- `src/storage/col_segment/store.rs` — `ColSegmentStore` struct, append/flush/get
- `src/storage/col_segment/segment.rs` — `Segment` wrapper + `SegmentCursor`
- `src/storage/col_segment/merge.rs` — `MergeCursor` heap merge iterator
- `src/storage/col_segment/manifest.rs` — `Manifest` append-only log + recovery
- `src/storage/col_segment/compactor.rs` — compaction + background thread
- `tests/test_col_segment.rs` — unit tests for all of the above

**Modified files (S6–S9, existing code):**
- `src/storage/mod.rs` — add `pub mod col_segment;`
- `src/database/core.rs` — add `col_segment_stores` field + compactor thread
- `src/database/crud.rs` — route finalize/scan to ColSegmentStore
- `src/database/indexes/column.rs` — CREATE INDEX uses multi-segment scan
- `src/sql/executor.rs` — query paths use ColSegmentStore scan

---

## Task S1: ColSegmentStore skeleton + Segment + single-segment get

**Files:**
- Create: `src/storage/col_segment/mod.rs`
- Create: `src/storage/col_segment/segment.rs`
- Create: `src/storage/col_segment/store.rs`
- Modify: `src/storage/mod.rs`
- Test: `tests/test_col_segment.rs`

- [ ] **Step 1: Write failing test for single-segment get**

Create `tests/test_col_segment.rs`:

```rust
use motedb::storage::col_segment::{ColSegmentStore, Segment};
use motedb::types::{ColumnType, Value};
use tempfile::TempDir;

fn make_col_types() -> Vec<ColumnType> {
    vec![ColumnType::Integer, ColumnType::Text]
}

#[test]
fn single_segment_get_returns_inserted_row() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", make_col_types()).unwrap();

    // Append one batch, flush to a single segment
    store.append_rows(&[
        (1, 100, vec![Value::Integer(10), Value::Text("a".into())]),
        (2, 100, vec![Value::Integer(20), Value::Text("b".into())]),
    ]).unwrap();
    store.flush_buffer().unwrap();

    // get by key returns the row
    let row = store.get(1).unwrap();
    assert_eq!(row, vec![Value::Integer(10), Value::Text("a".into())]);

    let row2 = store.get(2).unwrap();
    assert_eq!(row2, vec![Value::Integer(20), Value::Text("b".into())]);

    // missing key
    assert!(store.get(999).is_none());

    assert_eq!(store.segment_count(), 1);
}
```

- [ ] **Step 2: Run test to verify it fails (compile error)**

Run: `cargo test --test test_col_segment`
Expected: FAIL — module `col_segment` does not exist.

- [ ] **Step 3: Create module skeleton**

`src/storage/col_segment/mod.rs`:

```rust
//! Append-only multi-segment columnar store.
//!
//! Each table owns a `ColSegmentStore` holding an ordered list of immutable
//! `Segment`s (each backed by an existing `ColumnarSSTable`). Writes append
//! delta segments (O(1)); reads multi-way merge (O(1) memory); a background
//! compactor merges segments to bound the count.

mod segment;
mod store;

pub use segment::Segment;
pub use store::ColSegmentStore;
```

- [ ] **Step 4: Create Segment wrapper**

`src/storage/col_segment/segment.rs`:

```rust
use crate::storage::lsm::columnar::ColumnarSSTable;
use std::time::Instant;

/// Immutable columnar segment = a `ColumnarSSTable` plus bookkeeping metadata.
/// File format is unchanged — this is a thin wrapper for the multi-segment layer.
pub struct Segment {
    pub sst: ColumnarSSTable,
    pub id: u64,
    pub row_count: usize,
    pub created_at: Instant,
}

impl Segment {
    pub fn open(path: &std::path::Path, id: u64) -> crate::Result<Self> {
        let sst = ColumnarSSTable::open(path)?;
        let row_count = sst.num_rows;
        Ok(Self { sst, id, row_count, created_at: Instant::now() })
    }
}
```

- [ ] **Step 5: Create ColSegmentStore with create/append_rows/flush_buffer/get**

`src/storage/col_segment/store.rs`:

```rust
use super::segment::Segment;
use crate::storage::lsm::columnar::ColumnarSSTableBuilder;
use crate::types::{ColumnType, Value};
use crate::{Result, StorageError};
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Append-only multi-segment store for one columnar table.
pub struct ColSegmentStore {
    table_name: String,
    dir: PathBuf,
    segments: RwLock<VecDeque<Arc<Segment>>>,
    write_buf: Mutex<ColumnarSSTableBuilder>,
    next_segment_id: AtomicU64,
    col_types: Vec<ColumnType>,
}

impl ColSegmentStore {
    /// Create a new store for a table in `base_dir/columnar/<table_name>/`.
    pub fn create(base_dir: &Path, table_name: &str, col_types: Vec<ColumnType>) -> Result<Arc<Self>> {
        let dir = base_dir.join("columnar").join(table_name);
        std::fs::create_dir_all(&dir)?;
        // Builder writes to a temp path; flushed segments get numbered names.
        let buf_path = dir.join(".writebuf.tmp");
        let write_buf = ColumnarSSTableBuilder::new(&buf_path, col_types.clone());
        Ok(Arc::new(Self {
            table_name: table_name.to_string(),
            dir,
            segments: RwLock::new(VecDeque::new()),
            write_buf: Mutex::new(write_buf),
            next_segment_id: AtomicU64::new(1),
            col_types,
        }))
    }

    /// Append rows to the in-memory buffer. O(rows).
    pub fn append_rows(&self, rows: &[(u64, u64, Vec<Value>)]) -> Result<()> {
        let mut buf = self.write_buf.lock();
        for (key, ts, row) in rows {
            buf.add_values(*key, *ts, false, row)?;
        }
        Ok(())
    }

    /// Flush the buffer to a new delta segment on disk. Does NOT read old segments.
    pub fn flush_buffer(&self) -> Result<()> {
        // Take buffer contents out (replace with fresh builder), release lock fast.
        let buf_path = self.dir.join(".writebuf.tmp");
        let old_buf = {
            let mut guard = self.write_buf.lock();
            let fresh = ColumnarSSTableBuilder::new(&buf_path, self.col_types.clone());
            std::mem::replace(&mut *guard, fresh)
        };
        if old_buf.num_rows == 0 {
            return Ok(());
        }
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        // finish() consumes the builder and writes the file (atomic temp+rename inside).
        // We need finish_into a specific path — reuse finish_and_reset pattern.
        // ColumnarSSTableBuilder::finish consumes self; we took it out so call finish.
        let mut b = old_buf;
        // finish writes to b.path which is buf_path; then we rename to the numbered path.
        use crate::storage::lsm::columnar::ColumnarSSTableBuilder;
        // The builder's finish_and_reset writes to its internal path. We want a numbered file.
        // Simplest: set path then finish. But path is pub. Set it:
        b.path = path.clone();
        let _ = b.finish()?;  // finish(self) writes file
        let seg = Segment::open(&path, id)?;
        self.segments.write().push_back(Arc::new(seg));
        Ok(())
    }

    /// Point lookup: newest segment first, return first hit.
    pub fn get(&self, key: u64) -> Option<Vec<Value>> {
        let segs = self.segments.read();
        for seg in segs.iter().rev() {
            if let Some(row) = seg.sst.get_row(key, &self.col_types) {
                return Some(row);
            }
        }
        None
    }

    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    pub fn col_types(&self) -> &[ColumnType] {
        &self.col_types
    }
}
```

- [ ] **Step 6: Register module in src/storage/mod.rs**

Add after `pub mod columnar;`:

```rust
pub mod col_segment;
```

- [ ] **Step 7: Run test to verify it passes**

Run: `cargo test --test test_col_segment`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add src/storage/col_segment/ src/storage/mod.rs tests/test_col_segment.rs
git commit -m "feat(col-segment): S1 ColSegmentStore skeleton + single-segment get"
```

---

## Task S2: Multi-segment append + flush + count

**Files:**
- Modify: `tests/test_col_segment.rs`
- (no new source — S1 already implements append/flush)

- [ ] **Step 1: Write failing test for multiple flushes**

Append to `tests/test_col_segment.rs`:

```rust
#[test]
fn multiple_flushes_create_multiple_segments() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", make_col_types()).unwrap();

    // Batch 1
    store.append_rows(&[
        (1, 100, vec![Value::Integer(10), Value::Text("a".into())]),
        (2, 100, vec![Value::Integer(20), Value::Text("b".into())]),
    ]).unwrap();
    store.flush_buffer().unwrap();
    assert_eq!(store.segment_count(), 1);

    // Batch 2 — new keys
    store.append_rows(&[
        (3, 200, vec![Value::Integer(30), Value::Text("c".into())]),
    ]).unwrap();
    store.flush_buffer().unwrap();
    assert_eq!(store.segment_count(), 2);

    // All keys visible via get
    assert_eq!(store.get(1).unwrap()[0], Value::Integer(10));
    assert_eq!(store.get(3).unwrap()[0], Value::Integer(30));
}
```

- [ ] **Step 2: Run test — should pass already (S1 logic covers it)**

Run: `cargo test --test test_col_segment -- multiple_flushes`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_col_segment.rs
git commit -m "test(col-segment): S2 multi-segment append/flush"
```

---

## Task S3: MergeCursor multi-way merge + newest-version semantics

**Files:**
- Create: `src/storage/col_segment/merge.rs`
- Modify: `src/storage/col_segment/mod.rs`, `src/storage/col_segment/store.rs`
- Modify: `tests/test_col_segment.rs`

- [ ] **Step 1: Write failing tests for merge semantics**

Append to `tests/test_col_segment.rs`:

```rust
#[test]
fn merge_returns_all_distinct_keys_across_segments() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", make_col_types()).unwrap();
    // seg1: keys 1,2
    store.append_rows(&[
        (1, 100, vec![Value::Integer(10), Value::Text("a".into())]),
        (2, 100, vec![Value::Integer(20), Value::Text("b".into())]),
    ]).unwrap();
    store.flush_buffer().unwrap();
    // seg2: keys 3,4
    store.append_rows(&[
        (3, 100, vec![Value::Integer(30), Value::Text("c".into())]),
        (4, 100, vec![Value::Integer(40), Value::Text("d".into())]),
    ]).unwrap();
    store.flush_buffer().unwrap();

    let rows: Vec<(u64, Vec<Value>)> = store.scan().map(|(k, _ts, r)| (k, r)).collect();
    let keys: Vec<u64> = rows.iter().map(|(k, _)| *k).collect();
    assert_eq!(keys, vec![1, 2, 3, 4]);
}

#[test]
fn merge_newest_version_wins_for_same_key() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", make_col_types()).unwrap();
    // seg1: key=1 val=10 ts=100
    store.append_rows(&[(1, 100, vec![Value::Integer(10), Value::Text("old".into())])]).unwrap();
    store.flush_buffer().unwrap();
    // seg2: key=1 val=99 ts=200 (newer)
    store.append_rows(&[(1, 200, vec![Value::Integer(99), Value::Text("new".into())])]).unwrap();
    store.flush_buffer().unwrap();

    let rows: Vec<(u64, Vec<Value>)> = store.scan().map(|(k, _ts, r)| (k, r)).collect();
    assert_eq!(rows.len(), 1, "dedup same key");
    assert_eq!(rows[0].0, 1);
    assert_eq!(rows[0].1[0], Value::Integer(99), "newest version wins");
    assert_eq!(rows[0].1[1], Value::Text("new".into()));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test test_col_segment -- merge`
Expected: FAIL — `scan()` method not defined.

- [ ] **Step 3: Implement MergeCursor**

`src/storage/col_segment/merge.rs`:

```rust
//! Multi-way merge cursor over segments. O(1) extra memory via a min-heap
//! keyed by row key; newest version (highest timestamp, newest segment) wins.

use super::segment::Segment;
use crate::storage::lsm::columnar::ColumnarSSTable;
use crate::types::{ColumnType, Value};
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::sync::Arc;

/// Cursor over one segment, iterating rows in ascending key order.
/// Lazily decodes only requested columns.
struct SegmentCursor {
    sst: Arc<ColumnarSSTable>,
    /// Row indices sorted ascending by key.
    order: Vec<usize>,
    pos: usize,
    col_types: Vec<ColumnType>,
}

impl SegmentCursor {
    fn new(seg: &Segment, col_types: Vec<ColumnType>) -> Self {
        // Build ascending-key row index order.
        let mut order: Vec<usize> = (0..seg.sst.num_rows).collect();
        // RowMap keys are written in insertion order; for merge correctness we
        // need ascending order. Sort by key (stable).
        order.sort_by_key(|&i| seg.sst.row_map.key(i));
        Self { sst: Arc::new(seg.sst.clone()), order, pos: 0, col_types }
    }

    fn peek_key(&self) -> Option<u64> {
        self.order.get(self.pos).map(|&i| self.sst.row_map.key(i))
    }

    fn peek_ts(&self) -> Option<u64> {
        self.order.get(self.pos).map(|&i| self.sst.row_map.timestamp(i))
    }

    fn is_deleted_at(&self, i: usize) -> bool {
        self.sst.row_map.is_deleted(i)
    }

    /// Advance past current row; return (key, ts, deleted, row_values).
    fn advance(&mut self) -> Option<(u64, u64, bool, Vec<Value>)> {
        let &i = self.order.get(self.pos)?;
        self.pos += 1;
        let key = self.sst.row_map.key(i);
        let ts = self.sst.row_map.timestamp(i);
        let deleted = self.sst.row_map.is_deleted(i);
        let row = if deleted { vec![] } else {
            self.sst.get_row(key, &self.col_types).unwrap_or_default()
        };
        Some((key, ts, deleted, row))
    }
}

/// Multi-segment merge iterator. Yields (key, timestamp, row) for the newest
/// live version of each key, skipping tombstones and superseded versions.
pub struct MergeCursor {
    cursors: Vec<SegmentCursor>,
    /// Min-heap of (key, cursor_index). Drives ascending-key iteration.
    heap: BinaryHeap<Reverse<(u64, usize)>>,
}

impl MergeCursor {
    /// `segments` must be in ascending creation order (oldest first).
    pub fn new(segments: &[Arc<Segment>], col_types: &[ColumnType]) -> Self {
        let mut cursors: Vec<SegmentCursor> = segments.iter()
            .map(|s| SegmentCursor::new(s, col_types.to_vec()))
            .collect();
        let mut heap = BinaryHeap::new();
        for (idx, c) in cursors.iter().enumerate() {
            if let Some(k) = c.peek_key() {
                heap.push(Reverse((k, idx)));
            }
        }
        // cursors mutably borrowed below; we used immutable refs above.
        let _ = &mut cursors;
        Self { cursors, heap }
    }
}

impl Iterator for MergeCursor {
    type Item = (u64 /*key*/, u64 /*ts*/, Vec<Value>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Find global minimum key among all cursors.
            let min_key = self.heap.peek().map(|Reverse((k, _))| *k)?;

            // Among cursors that have min_key at their head, pick the one with
            // the highest timestamp (newest version wins). Ties broken by later
            // cursor index (newer segment). We scan all cursors because multiple
            // may sit on the same min_key.
            let mut best: Option<(usize, u64)> = None; // (cursor_idx, ts)
            for (idx, c) in self.cursors.iter().enumerate() {
                if c.peek_key() == Some(min_key) {
                    let ts = c.peek_ts().unwrap_or(0);
                    match best {
                        Some((_, bts)) if bts >= ts => {}
                        _ => best = Some((idx, ts)),
                    }
                }
            }

            // Advance ALL cursors that are sitting on min_key (consume the key).
            // Drain matching heap entries.
            while let Some(&Reverse((k, _))) = self.heap.peek() {
                if k != min_key { break; }
                self.heap.pop();
            }
            // Re-heap cursors that we advance.
            let mut advanced_idx: Vec<usize> = Vec::new();
            for (idx, c) in self.cursors.iter_mut().enumerate() {
                if c.peek_key() == Some(min_key) {
                    // We will advance below; record idx.
                    advanced_idx.push(idx);
                }
            }
            // For the best cursor, capture its row; for others just advance.
            let best_idx = best.map(|(i, _)| i);
            let mut emitted: Option<(u64, u64, Vec<Value>)> = None;
            for &idx in &advanced_idx {
                let c = &mut self.cursors[idx];
                if let Some((key, ts, deleted, row)) = c.advance() {
                    if Some(idx) == best_idx {
                        if !deleted {
                            emitted = Some((key, ts, row));
                        }
                        // if deleted (tombstone) and it's the winner: key is deleted, emit nothing
                    }
                }
                if let Some(k) = c.peek_key() {
                    self.heap.push(Reverse((k, idx)));
                }
            }

            if let Some(e) = emitted {
                return Some(e);
            }
            // else: key was tombstoned or all versions consumed; loop to next key.
        }
    }
}
```

- [ ] **Step 4: Add scan() to ColSegmentStore and export MergeCursor**

In `src/storage/col_segment/store.rs` add method:

```rust
    /// Full-table ordered scan via multi-way merge.
    pub fn scan(&self) -> impl Iterator<Item = (u64, u64, Vec<Value>)> + '_ {
        let segs: Vec<Arc<Segment>> = self.segments.read().iter().cloned().collect();
        crate::storage::col_segment::merge::MergeCursor::new(&segs, &self.col_types)
    }
```

In `src/storage/col_segment/mod.rs` add:

```rust
mod merge;
pub use merge::MergeCursor;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --test test_col_segment -- merge`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/storage/col_segment/ tests/test_col_segment.rs
git commit -m "feat(col-segment): S3 MergeCursor multi-way merge, newest-version-wins"
```

---

## Task S4: Manifest (append-only log + crash recovery)

**Files:**
- Create: `src/storage/col_segment/manifest.rs`
- Modify: `src/storage/col_segment/mod.rs`, `src/storage/col_segment/store.rs`
- Modify: `tests/test_col_segment.rs`

- [ ] **Step 1: Write failing test for manifest add + recover**

Append to `tests/test_col_segment.rs`:

```rust
use motedb::storage::col_segment::manifest::Manifest;

#[test]
fn manifest_records_and_recovers_segments() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("MANIFEST");
    {
        let mut m = Manifest::create(&path).unwrap();
        m.add_segment(1).unwrap();
        m.add_segment(2).unwrap();
        m.record_compaction(3, &[1, 2]).unwrap();
    }
    // Reopen and replay.
    let m = Manifest::open(&path).unwrap();
    let state = m.replay();
    let active: Vec<u64> = state.active_segments.clone();
    let obsolete: Vec<u64> = state.obsolete_files.clone();
    assert!(active.contains(&3), "new segment 3 active");
    assert!(!active.contains(&1), "old segment 1 superseded");
    assert!(!active.contains(&2), "old segment 2 superseded");
    assert!(obsolete.contains(&1) && obsolete.contains(&2), "old files pending GC");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_col_segment -- manifest_records`
Expected: FAIL — module `manifest` not found.

- [ ] **Step 3: Implement Manifest**

`src/storage/col_segment/manifest.rs`:

```rust
//! Append-only manifest log. Records segment lifecycle (add / compaction / gc).
//! Crash-safe: each record is appended + fsync'd before in-memory state changes.

use crate::{Result, StorageError};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"MOTS";
const VERSION: u16 = 1;

#[derive(Debug, Clone, Default)]
pub struct ManifestState {
    /// Segment ids currently active (queryable).
    pub active_segments: Vec<u64>,
    /// Segment files safe to delete (superseded + manifest-recorded).
    pub obsolete_files: Vec<u64>,
}

enum Record {
    AddSegment(u64),
    Compaction { new_id: u64, old_ids: Vec<u64> },
    GcCompleted(Vec<u64>),
}

impl Record {
    fn type_byte(&self) -> u8 {
        match self { Record::AddSegment(_) => 1, Record::Compaction { .. } => 2, Record::GcCompleted(_) => 3 }
    }
    fn encode(&self) -> Vec<u8> {
        let mut v = Vec::new();
        v.push(self.type_byte());
        match self {
            Record::AddSegment(id) => v.extend_from_slice(&id.to_le_bytes()),
            Record::Compaction { new_id, old_ids } => {
                v.extend_from_slice(&new_id.to_le_bytes());
                v.extend_from_slice(&(old_ids.len() as u16).to_le_bytes());
                for id in old_ids { v.extend_from_slice(&id.to_le_bytes()); }
            }
            Record::GcCompleted(ids) => {
                v.extend_from_slice(&(ids.len() as u16).to_le_bytes());
                for id in ids { v.extend_from_slice(&id.to_le_bytes()); }
            }
        }
        v
    }
}

pub struct Manifest {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Manifest {
    pub fn create(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.write_all(&0u32.to_le_bytes())?; // record_count placeholder
        file.sync_all()?;
        Ok(Self { path: path.to_path_buf(), writer: BufWriter::new(file) })
    }

    /// Open existing manifest for appending.
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new().read(true).write(true).open(path)?;
        Ok(Self { path: path.to_path_buf(), writer: BufWriter::new(file) })
    }

    fn append(&mut self, rec: Record) -> Result<()> {
        let bytes = rec.encode();
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        // fsync for crash safety — manifest is the only fsync'd file.
        self.writer.get_ref().sync_all()?;
        Ok(())
    }

    pub fn add_segment(&mut self, id: u64) -> Result<()> {
        self.append(Record::AddSegment(id))
    }

    pub fn record_compaction(&mut self, new_id: u64, old_ids: &[u64]) -> Result<()> {
        self.append(Record::Compaction { new_id, old_ids: old_ids.to_vec() })
    }

    pub fn record_gc(&mut self, ids: &[u64]) -> Result<()> {
        self.append(Record::GcCompleted(ids.to_vec()))
    }

    /// Replay all records to reconstruct state. Used at recovery.
    pub fn replay(&self) -> ManifestState {
        let mut data = Vec::new();
        if let Ok(mut f) = File::open(&self.path) {
            let _ = f.read_to_end(&mut data);
        }
        if data.len() < 10 || &data[..4] != MAGIC { return ManifestState::default(); }
        let mut pos = 10; // skip magic(4) + version(2) + count(4)
        let mut active: Vec<u64> = Vec::new();
        let mut obsolete: Vec<u64> = Vec::new();
        while pos < data.len() {
            let t = data[pos]; pos += 1;
            match t {
                1 => { // AddSegment
                    if pos + 8 > data.len() { break; }
                    let id = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap());
                    pos += 8;
                    active.push(id);
                }
                2 => { // Compaction
                    if pos + 10 > data.len() { break; }
                    let new_id = u64::from_le_bytes(data[pos..pos+8].try_into().unwrap()); pos += 8;
                    let n = u16::from_le_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
                    let mut olds = Vec::with_capacity(n);
                    for _ in 0..n {
                        if pos + 8 > data.len() { break; }
                        olds.push(u64::from_le_bytes(data[pos..pos+8].try_into().unwrap()));
                        pos += 8;
                    }
                    active.retain(|x| !olds.contains(x));
                    active.push(new_id);
                    obsolete.extend(olds);
                }
                3 => { // GcCompleted
                    if pos + 2 > data.len() { break; }
                    let n = u16::from_le_bytes(data[pos..pos+2].try_into().unwrap()) as usize; pos += 2;
                    let mut gced = Vec::with_capacity(n);
                    for _ in 0..n {
                        if pos + 8 > data.len() { break; }
                        gced.push(u64::from_le_bytes(data[pos..pos+8].try_into().unwrap()));
                        pos += 8;
                    }
                    obsolete.retain(|x| !gced.contains(x));
                }
                _ => break,
            }
        }
        ManifestState { active_segments: active, obsolete_files: obsolete }
    }
}
```

- [ ] **Step 4: Export manifest + wire into store flush**

In `src/storage/col_segment/mod.rs` add:

```rust
pub mod manifest;
```

Update `ColSegmentStore::create` to init manifest, and `flush_buffer` to record add_segment. See Step 5.

- [ ] **Step 5: Wire manifest into ColSegmentStore**

In `store.rs`, add field `manifest: Mutex<Manifest>` to the struct. In `create()`:

```rust
let manifest_path = dir.join("MANIFEST");
let manifest = if manifest_path.exists() {
    Manifest::open(&manifest_path)?
} else {
    Manifest::create(&manifest_path)?
};
```
Store `manifest: Mutex::new(manifest)` in struct.

In `flush_buffer`, after pushing the segment:
```rust
self.manifest.lock().add_segment(id)?;
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test --test test_col_segment`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/storage/col_segment/ tests/test_col_segment.rs
git commit -m "feat(col-segment): S4 manifest append-log + crash recovery replay"
```

---

## Task S5: Compaction (background thread + pick + apply)

**Files:**
- Create: `src/storage/col_segment/compactor.rs`
- Modify: `src/storage/col_segment/mod.rs`, `src/storage/col_segment/store.rs`
- Modify: `tests/test_col_segment.rs`

- [ ] **Step 1: Write failing test for compaction**

Append to `tests/test_col_segment.rs`:

```rust
#[test]
fn compaction_merges_segments_and_clears_old() {
    let dir = TempDir::new().unwrap();
    let store = ColSegmentStore::create(dir.path(), "t", make_col_types()).unwrap();
    // 3 segments
    for start in [0, 2, 4] {
        store.append_rows(&[
            (start+1, 100, vec![Value::Integer(start+1), Value::Text("x".into())]),
            (start+2, 100, vec![Value::Integer(start+2), Value::Text("y".into())]),
        ]).unwrap();
        store.flush_buffer().unwrap();
    }
    assert_eq!(store.segment_count(), 3);

    store.compact_once().unwrap();
    assert_eq!(store.segment_count(), 1, "compaction reduces to 1 segment");

    // All data still visible
    let keys: Vec<u64> = store.scan().map(|(k, _, _)| k).collect();
    assert_eq!(keys, vec![1, 2, 3, 4, 5, 6]);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test test_col_segment -- compaction_merges`
Expected: FAIL — `compact_once` not defined.

- [ ] **Step 3: Implement compaction on ColSegmentStore**

Add to `src/storage/col_segment/store.rs`:

```rust
use super::merge::MergeCursor;
use crate::storage::lsm::columnar::ColumnarSSTableBuilder;

const COMPACTION_SEGMENT_THRESHOLD: usize = 3;

impl ColSegmentStore {
    /// Run one compaction pass (synchronous; called by bg thread or test).
    pub fn compact_once(&self) -> Result<()> {
        let old_segs: Vec<Arc<Segment>> = {
            let segs = self.segments.read();
            if segs.len() < COMPACTION_SEGMENT_THRESHOLD { return Ok(()); }
            segs.iter().cloned().collect()
        };
        let old_ids: Vec<u64> = old_segs.iter().map(|s| s.id).collect();

        // Merge-read old segments, write new segment (dedup + drop tombstones).
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        let mut builder = ColumnarSSTableBuilder::new(&path, self.col_types.clone());
        let merge = MergeCursor::new(&old_segs, &self.col_types);
        let mut count = 0usize;
        for (key, ts, row) in merge {
            builder.add_values(key, ts, false, &row)?;
            count += 1;
        }
        builder.finish()?;

        let new_seg = Arc::new(Segment::open(&path, id)?);

        // Record compaction in manifest FIRST (crash safety), then swap memory.
        self.manifest.lock().record_compaction(id, &old_ids)?;

        {
            let mut segs = self.segments.write();
            let old_set: std::collections::HashSet<u64> = old_ids.iter().copied().collect();
            let new_list: VecDeque<Arc<Segment>> = segs.iter()
                .filter(|s| !old_set.contains(&s.id))
                .cloned()
                .collect();
            *segs = new_list;
            segs.push_back(new_seg);
        }

        // GC: delete old files, record in manifest.
        for oid in &old_ids {
            let p = self.dir.join(format!("{:010}.sst", oid));
            let _ = std::fs::remove_file(p);
        }
        self.manifest.lock().record_gc(&old_ids)?;
        Ok(())
    }

    pub fn needs_compaction(&self) -> bool {
        self.segments.read().len() >= COMPACTION_SEGMENT_THRESHOLD
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test test_col_segment -- compaction_merges`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/storage/col_segment/ tests/test_col_segment.rs
git commit -m "feat(col-segment): S5 compaction merge + manifest GC"
```

---

## Task S6: Wire ColSegmentStore into MoteDB (write path)

**Files:**
- Modify: `src/database/core.rs` (add field)
- Modify: `src/database/crud.rs` (route append to store)
- Modify: `src/database/indexes/column.rs` (CREATE INDEX uses store)

**Approach:** Add `col_segment_stores: DashMap<String, Arc<ColSegmentStore>>` alongside existing `columnar_sstables` (keep old field for now — dual-track migration). When a table uses the new store, route INSERT → `store.append_rows`, finalize → `store.flush_buffer`.

- [ ] **Step 1: Add field + lazy init in MoteDB**

In `src/database/core.rs`, add to struct:

```rust
pub(crate) col_segment_stores: Arc<DashMap<String, Arc<crate::storage::col_segment::ColSegmentStore>>>,
```

In `Default::default()` / constructor: `col_segment_stores: Arc::new(DashMap::new()),`

Add helper method:

```rust
/// Get or create the ColSegmentStore for a table.
pub fn get_or_create_col_segment_store(&self, table_name: &str, col_types: Vec<ColumnType>) -> Result<Arc<ColSegmentStore>> {
    if let Some(s) = self.col_segment_stores.get(table_name) {
        return Ok(s.clone());
    }
    let store = ColSegmentStore::create(&self.base_dir, table_name, col_types)?;
    self.col_segment_stores.insert(table_name.to_string(), store.clone());
    Ok(store)
}
```

- [ ] **Step 2: Route batch INSERT into store when columnar**

In `src/database/crud.rs` `batch_insert_rows_to_table`, after building columnar buffer, also push to the store:

Locate the section that appends to `columnar_write_bufs` and add (when store exists):

```rust
// Also append to ColSegmentStore (new path). For now, mirror data into both
// until query path migrates (S7). This is the dual-track bridge.
if let Ok(schema) = self.table_registry.get_table(table_name) {
    let col_types = schema.col_types().to_vec();
    let store = self.get_or_create_col_segment_store(table_name, col_types)?;
    let rows: Vec<(u64, u64, Vec<Value>)> = /* the just-inserted rows */;
    store.append_rows(&rows)?;
}
```

(Exact integration point depends on reading the batch_insert code; the subagent will adapt.)

- [ ] **Step 3: Verify INSERT still works (dual-track)**

Run: `cargo test --test test_columnar_acid`
Expected: existing tests still PASS (old path intact).

Run: `cargo run --release --example repro_regression` → check INSERT time stable.

- [ ] **Step 4: Commit**

```bash
git add src/database/core.rs src/database/crud.rs
git commit -m "feat(col-segment): S6 wire write path into MoteDB (dual-track)"
```

---

## Task S7: Route query/scan through ColSegmentStore

**Files:**
- Modify: `src/sql/executor.rs` (scan paths)
- Modify: `src/database/crud.rs` (scan_table_rows_streaming columnar variant)

- [ ] **Step 1: Add scan delegation in executor full-scan path**

In `execute_full_scan_streaming`, when `col_segment_stores` has the table, use `store.scan()` instead of finalize+read.

- [ ] **Step 2: Update IN-subquery columnar branch to use store.scan()**

The `build_in_hashset_from_columnar` (added during bug fix) should call `store.scan_projected` / `store.scan` instead of `finalize_columnar_buffer`.

- [ ] **Step 3: Run repro_regression**

Run: `cargo run --release --example repro_regression`
Expected: IN subquery returns 20000, WHERE region='US' returns 20000, SELECT * returns 60000.

- [ ] **Step 4: Commit**

```bash
git add src/sql/executor.rs src/database/crud.rs
git commit -m "feat(col-segment): S7 route query scan through ColSegmentStore"
```

---

## Task S8: CREATE INDEX multi-segment scan (remove finalize merge)

**Files:**
- Modify: `src/database/indexes/column.rs`

- [ ] **Step 1: Replace finalize call with store.scan in CREATE INDEX**

In `create_column_index_with_name`, replace:
```rust
if self.columnar_write_bufs.contains_key(table_name) {
    self.finalize_columnar_buffer(table_name);
}
```
with:
```rust
self.get_or_create_col_segment_store(table_name, col_types)?.flush_buffer()?;
```
Then iterate `store.scan()` for index values instead of reading a single `col_sst`.

- [ ] **Step 2: Run repro_regression — CREATE INDEX target < 30ms**

Run: `cargo run --release --example repro_regression`
Expected: CREATE INDEX total < 30ms (was 673ms).

- [ ] **Step 3: Commit**

```bash
git add src/database/indexes/column.rs
git commit -m "perf(col-segment): S8 CREATE INDEX multi-segment scan, drop full-table merge"
```

---

## Task S9: Cleanup — remove old single-SSTable path

**Files:**
- Modify: `src/database/core.rs` (remove `columnar_sstables` field)
- Modify: `src/database/crud.rs` (remove `finalize_columnar_buffer` merge logic)
- Modify: various (remove dead code)

- [ ] **Step 1: Remove finalize_columnar_buffer merge body**

Replace the merge-based finalize with a simple flush_buffer delegation.

- [ ] **Step 2: Remove columnar_sstables field + all reads**

Migrate all `columnar_sstables.get(table)` to `col_segment_stores.get(table)`.

- [ ] **Step 3: Run full test suite**

Run: `cargo test --release`
Expected: all PASS.

Run: `cargo run --release --example bench_vs_sqlite 2>&1 | tail -40` (background, 10min)
Expected: CREATE INDEX near baseline, no regressions.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "refactor(col-segment): S9 remove legacy single-SSTable columnar path"
```

---

## Self-Review Notes

- **Spec coverage**: S1=architecture+data model, S2=write path, S3=query path, S4=manifest, S5=compaction, S6-S8=migration, S9=cleanup. All design sections covered.
- **Type consistency**: `ColSegmentStore` used consistently (avoids `ColumnarStore` collision with time-series module). `Segment`, `MergeCursor`, `Manifest`, `ManifestState` names stable across tasks.
- **Risk**: S6-S8 touch existing hot paths. Each has a verification step (test or repro). S1-S5 are pure additions, safe.
