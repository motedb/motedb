use super::segment::Segment;
use super::merge::MergeCursor;
use super::manifest::Manifest;
use crate::storage::lsm::columnar::ColumnarSSTableBuilder;
use crate::types::{ColumnType, Value};
use crate::Result;
use parking_lot::{Mutex, RwLock};
use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Compaction trigger: merge when segment count reaches this.
const COMPACTION_SEGMENT_THRESHOLD: usize = 3;

/// Append-only multi-segment store for one columnar table.
pub struct ColSegmentStore {
    table_name: String,
    dir: PathBuf,
    /// Active segments in ascending creation order (oldest first, newest at back).
    segments: RwLock<VecDeque<Arc<Segment>>>,
    /// In-memory write buffer. Flushed as a delta segment (does not read old data).
    write_buf: Mutex<ColumnarSSTableBuilder>,
    next_segment_id: AtomicU64,
    manifest: Mutex<Manifest>,
    col_types: Vec<ColumnType>,
}

impl ColSegmentStore {
    /// Create a new store for a table at `base_dir/columnar_ms/<table_name>/`.
    /// (`columnar_ms` to avoid clashing with the time-series `columnar/` dir.)
    pub fn create(base_dir: &Path, table_name: &str, col_types: Vec<ColumnType>) -> Result<Arc<Self>> {
        let dir = base_dir.join("columnar_ms").join(table_name);
        std::fs::create_dir_all(&dir)?;
        let manifest_path = dir.join("MANIFEST");
        let manifest = if manifest_path.exists() {
            Manifest::open(&manifest_path)?
        } else {
            Manifest::create(&manifest_path)?
        };
        let buf_path = dir.join(".writebuf.tmp");
        let write_buf = ColumnarSSTableBuilder::new(&buf_path, col_types.clone());
        Ok(Arc::new(Self {
            table_name: table_name.to_string(),
            dir,
            segments: RwLock::new(VecDeque::new()),
            write_buf: Mutex::new(write_buf),
            next_segment_id: AtomicU64::new(1),
            manifest: Mutex::new(manifest),
            col_types,
        }))
    }

    /// Append rows to the in-memory buffer. O(rows). Each tuple: (key, timestamp, values).
    pub fn append_rows(&self, rows: &[(u64, u64, Vec<Value>)]) -> Result<()> {
        let mut buf = self.write_buf.lock();
        for (key, ts, row) in rows {
            buf.add_values(*key, *ts, false, row)?;
        }
        Ok(())
    }

    /// Flush the buffer to a new delta segment on disk. Does NOT read old segments.
    /// O(this batch). Writes the file (no fsync — durability via WAL/manifest).
    pub fn flush_buffer(&self) -> Result<()> {
        // Take buffer contents out, replace with a fresh builder, release the lock fast.
        let buf_path = self.dir.join(".writebuf.tmp");
        let mut old_buf = {
            let mut guard = self.write_buf.lock();
            let fresh = ColumnarSSTableBuilder::new(&buf_path, self.col_types.clone());
            std::mem::replace(&mut *guard, fresh)
        };
        if old_buf.num_rows == 0 {
            return Ok(());
        }
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        // finish() writes to builder.path; set it to the numbered path first.
        old_buf.path = path.clone();
        old_buf.finish()?;

        let seg = Arc::new(Segment::open(&path, id)?);
        // Record in manifest (fsync'd) BEFORE exposing in memory.
        self.manifest.lock().add_segment(id)?;
        self.segments.write().push_back(seg);
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

    /// Full-table ordered scan via multi-way merge. Newest version wins.
    pub fn scan(&self) -> MergeCursor {
        let segs: Vec<Arc<Segment>> = self.segments.read().iter().cloned().collect();
        MergeCursor::new(&segs, &self.col_types)
    }

    pub fn segment_count(&self) -> usize {
        self.segments.read().len()
    }

    pub fn col_types(&self) -> &[ColumnType] {
        &self.col_types
    }

    pub fn needs_compaction(&self) -> bool {
        self.segments.read().len() >= COMPACTION_SEGMENT_THRESHOLD
    }

    /// Run one compaction pass (synchronous; called by bg thread or test).
    /// Merges all active segments into one, deduplicating keys and dropping
    /// tombstoned/superseded versions.
    pub fn compact_once(&self) -> Result<()> {
        let old_segs: Vec<Arc<Segment>> = {
            let segs = self.segments.read();
            if segs.len() < COMPACTION_SEGMENT_THRESHOLD {
                return Ok(());
            }
            segs.iter().cloned().collect()
        };
        let old_ids: Vec<u64> = old_segs.iter().map(|s| s.id).collect();

        // Merge-read old segments, write a new segment (dedup + drop tombstones).
        let id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        let path = self.dir.join(format!("{:010}.sst", id));
        let mut builder = ColumnarSSTableBuilder::new(&path, self.col_types.clone());
        let merge = MergeCursor::new(&old_segs, &self.col_types);
        for (key, ts, row) in merge {
            builder.add_values(key, ts, false, &row)?;
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
}
