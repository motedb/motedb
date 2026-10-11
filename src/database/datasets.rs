//! Dataset snapshots (v0.14 W7): `COMMIT DATASET`, `DROP DATASET`,
//! `EXPORT DATASET`.
//!
//! A dataset commit pins the CURRENT segment set of every user table:
//! segments are immutable files, so the snapshot is just a list of segment
//! ids — near-free to take, and the GC filter (`ColSegmentStore::
//! set_gc_filter`) keeps compaction from physically deleting pinned files.
//! The manifest persists in the `_dataset_commits` table (one row per
//! dataset/table/segment); the in-memory `dataset_refs` map is the GC hot
//! path and is reloaded from that table on open.
//!
//! `EXPORT DATASET 'name' TO 'path'` materializes the snapshot into a fresh
//! standalone database: open the manifest segments (in-memory handle or
//! directly from disk via `Segment::open`), stream live rows into the new
//! DB with the same schema (indexes rebuild there via the async pipeline).

use crate::database::core::MoteDB;
use crate::storage::col_segment::Segment;
use crate::types::{EpisodeConfig, TableSchema, Value};
use std::collections::{HashMap, HashSet};
use std::path::Path;

const DATASET_MANIFEST_TABLE: &str = "_dataset_commits";

impl MoteDB {
    /// Reload the in-memory GC reference set from the manifest table
    /// (called on open; no-op when the table doesn't exist yet).
    pub(crate) fn load_dataset_refs(&self) {
        if self
            .table_registry
            .get_table(DATASET_MANIFEST_TABLE)
            .is_err()
        {
            return;
        }
        let iter = match self.scan_table_rows_streaming(DATASET_MANIFEST_TABLE) {
            Ok(i) => i,
            Err(_) => return,
        };
        for item in iter.flatten() {
            let (_rid, row) = item;
            // schema: (dataset TEXT, table_name TEXT, seg_id INTEGER, …)
            let (Some(Value::Text(_d)), Some(Value::Text(t)), Some(Value::Integer(id))) = (
                row.get(0).cloned(),
                row.get(1).cloned(),
                row.get(2).cloned(),
            ) else {
                continue;
            };
            let t_str = t.as_str().to_string();
            self.dataset_refs
                .entry(t_str)
                .or_default()
                .insert(id as u64);
        }
    }

    /// Ensure the manifest table exists (no-op when present).
    fn ensure_dataset_manifest(&self) -> crate::Result<()> {
        if self
            .table_registry
            .get_table(DATASET_MANIFEST_TABLE)
            .is_ok()
        {
            return Ok(());
        }
        let schema = TableSchema::new(
            DATASET_MANIFEST_TABLE.to_string(),
            vec![
                crate::types::ColumnDef::new(
                    "dataset".to_string(),
                    crate::types::ColumnType::Text,
                    0,
                ),
                crate::types::ColumnDef::new(
                    "table_name".to_string(),
                    crate::types::ColumnType::Text,
                    1,
                ),
                crate::types::ColumnDef::new(
                    "seg_id".to_string(),
                    crate::types::ColumnType::Integer,
                    2,
                ),
                crate::types::ColumnDef::new("note".to_string(), crate::types::ColumnType::Text, 3),
                crate::types::ColumnDef::new(
                    "committed_at".to_string(),
                    crate::types::ColumnType::Timestamp,
                    4,
                ),
            ],
        );
        self.create_table(schema)
    }

    /// Native manifest row insert (no SQL layer in database/).
    fn manifest_insert(
        &self,
        dataset: &str,
        table: &str,
        seg_id: u64,
        note: &str,
        now: i64,
    ) -> crate::Result<()> {
        self.insert_row_to_table(
            DATASET_MANIFEST_TABLE,
            vec![
                Value::text(dataset.to_string()),
                Value::text(table.to_string()),
                Value::Integer(seg_id as i64),
                Value::text(note.to_string()),
                Value::Timestamp(crate::types::Timestamp::from_micros(now)),
            ],
        )
        .map(|_| ())
    }

    /// 🔖 COMMIT DATASET 'name' — pin the current segment set of every user
    /// table. Returns (tables, segments) pinned.
    pub fn commit_dataset(&self, name: &str, note: &str) -> crate::Result<(usize, usize)> {
        if name.starts_with('_') || name.is_empty() {
            return Err(crate::error::StorageError::InvalidData(
                "dataset name must be non-empty and not start with '_'".into(),
            ));
        }
        self.ensure_dataset_manifest()?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_micros() as i64)
            .unwrap_or(0);

        let name_for_rows = name.to_string();
        let mut tables_pinned = 0usize;
        let mut segs_pinned = 0usize;
        for t in self.list_tables()? {
            if t.starts_with('_') {
                continue; // meta tables are not dataset content
            }
            let Some(store) = self.get_col_segment_store(&t) else {
                continue; // empty/legacy table — nothing to pin
            };
            // Durable segments before snapshotting: buffer rows must be in
            // segment files (the snapshot references FILES).
            let _ = store.flush_buffer();
            let _ = store.sync_manifest();
            let ids: HashSet<u64> = store.segments_snapshot().iter().map(|s| s.id).collect();
            for id in &ids {
                self.manifest_insert(&name_for_rows, t.as_str(), *id, note, now)?;
                self.dataset_refs.entry(t.clone()).or_default().insert(*id);
            }
            if !ids.is_empty() {
                tables_pinned += 1;
                segs_pinned += ids.len();
            }
        }
        Ok((tables_pinned, segs_pinned))
    }

    /// 🔖 DROP DATASET 'name' — release the pin (manifest rows deleted;
    /// in-memory refs rebuilt). Files become GC-eligible at the next
    /// compaction + sync point.
    pub fn drop_dataset(&self, name: &str) -> crate::Result<bool> {
        if self
            .table_registry
            .get_table(DATASET_MANIFEST_TABLE)
            .is_err()
        {
            return Ok(false);
        }
        // Native delete: scan the manifest, remove rows of this dataset.
        let mut victims: Vec<(u64, Vec<Value>)> = Vec::new();
        let iter = self.scan_table_rows_streaming(DATASET_MANIFEST_TABLE)?;
        for item in iter.flatten() {
            let (rid, row) = item;
            if matches!(row.first(), Some(Value::Text(d)) if d.as_str() == name) {
                victims.push((rid, row));
            }
        }
        if victims.is_empty() {
            return Ok(false);
        }
        self.delete_rows_batch(DATASET_MANIFEST_TABLE, victims)?;
        // Rebuild the ref set wholesale (cheap; keeps semantics simple).
        self.dataset_refs.clear();
        self.load_dataset_refs();
        Ok(true)
    }

    /// 🔖 EXPORT DATASET 'name' TO 'path' — materialize the snapshot into a
    /// fresh standalone database at `path` (must not exist). Returns
    /// (tables, rows) written.
    pub fn export_dataset(&self, name: &str, dest: &Path) -> crate::Result<(usize, usize)> {
        // 1. Resolve the manifest.
        if self
            .table_registry
            .get_table(DATASET_MANIFEST_TABLE)
            .is_err()
        {
            return Err(crate::error::StorageError::InvalidData(format!(
                "dataset '{}' not found (no manifest table)",
                name
            )));
        }
        let mut manifest: Vec<(String, HashSet<u64>)> = Vec::new(); // first-seen table order
        let mut idx: HashMap<String, usize> = HashMap::new();
        let iter = self.scan_table_rows_streaming(DATASET_MANIFEST_TABLE)?;
        for item in iter.flatten() {
            let (_rid, row) = item;
            let (Some(Value::Text(d)), Some(Value::Text(t)), Some(Value::Integer(id))) = (
                row.get(0).cloned(),
                row.get(1).cloned(),
                row.get(2).cloned(),
            ) else {
                continue;
            };
            if d.as_str() != name {
                continue;
            }
            let t_str = t.as_str().to_string();
            let i = match idx.get(t_str.as_str()) {
                Some(i) => *i,
                None => {
                    let i = manifest.len();
                    manifest.push((t_str.clone(), HashSet::new()));
                    idx.insert(t_str, i);
                    i
                }
            };
            manifest[i].1.insert(id as u64);
        }
        if manifest.is_empty() {
            return Err(crate::error::StorageError::InvalidData(format!(
                "dataset '{}' not found",
                name
            )));
        }
        if dest.exists() {
            return Err(crate::error::StorageError::InvalidData(format!(
                "export destination already exists: {}",
                dest.display()
            )));
        }

        // 2. Fresh destination DB.
        let dest_db = MoteDB::create(dest)?;

        // 3. Per table: create schema (indexes stripped — the dest pipeline
        // rebuilds them if the user re-declares; data fidelity first).
        let mut total_rows = 0usize;
        for (table, seg_ids) in &manifest {
            let schema = self.get_table_schema(table)?;
            let mut out_schema = (*schema).clone();
            out_schema.indexes = Vec::new();
            dest_db.create_table(out_schema.clone())?;

            let store = self.get_col_segment_store(table);
            let dir = store.as_ref().map(|s| s.dir_path());
            // Resolve each pinned segment: live handle or open from disk.
            let mut handles: Vec<std::sync::Arc<Segment>> = Vec::new();
            if let Some(store) = &store {
                for seg in store.segments_snapshot() {
                    if seg_ids.contains(&seg.id) {
                        handles.push(seg);
                    }
                }
            }
            for id in seg_ids {
                if handles.iter().any(|h| h.id == *id) {
                    continue;
                }
                let Some(dir) = &dir else {
                    return Err(crate::error::StorageError::InvalidData(format!(
                        "table '{table}': pinned segment {} unavailable",
                        id
                    )));
                };
                let p = dir.join(format!("{:010}.sst", id));
                handles.push(std::sync::Arc::new(Segment::open(&p, *id)?));
            }

            // Stream live rows into the destination.
            let col_types = schema.col_types().to_vec();
            let mut batch: Vec<Vec<Value>> = Vec::with_capacity(500);
            for seg in &handles {
                for i in 0..seg.sst.num_rows {
                    if seg.sst.row_map.is_deleted(i) {
                        continue;
                    }
                    let row = seg.get_row_at_idx(i, &col_types);
                    batch.push(row);
                    if batch.len() >= 500 {
                        for r in batch.drain(..) {
                            dest_db.insert_row_to_table(table, r)?;
                        }
                    }
                }
            }
            for r in batch {
                dest_db.insert_row_to_table(table, r)?;
            }
            total_rows += handles.iter().map(|h| h.row_count).sum::<usize>();
        }
        // Make the export durable immediately (and settle buffered rows so
        // the destination's first reads see a consistent single copy).
        dest_db.checkpoint()?;
        Ok((manifest.len(), total_rows))
    }

    /// List datasets (name → tables, first-seen order). Used by
    /// `DATASETS()`-style introspection and tests.
    pub fn list_datasets(&self) -> crate::Result<Vec<(String, Vec<String>)>> {
        let mut out: Vec<(String, Vec<String>)> = Vec::new();
        let mut idx: HashMap<String, usize> = HashMap::new();
        if self
            .table_registry
            .get_table(DATASET_MANIFEST_TABLE)
            .is_err()
        {
            return Ok(out);
        }
        let iter = self.scan_table_rows_streaming(DATASET_MANIFEST_TABLE)?;
        for item in iter.flatten() {
            let (_rid, row) = item;
            let (Some(Value::Text(d)), Some(Value::Text(t))) =
                (row.get(0).cloned(), row.get(1).cloned())
            else {
                continue;
            };
            let d_str = d.as_str().to_string();
            let i = match idx.get(d_str.as_str()) {
                Some(i) => *i,
                None => {
                    let i = out.len();
                    out.push((d_str.clone(), Vec::new()));
                    idx.insert(d_str, i);
                    i
                }
            };
            let t_str = t.as_str().to_string();
            if !out[i].1.contains(&t_str) {
                out[i].1.push(t_str);
            }
        }
        Ok(out)
    }

    /// Episode config re-export for tests/introspection.
    pub fn episode_config_of(&self, table: &str) -> Option<EpisodeConfig> {
        (*self.get_table_schema(table).ok()?).episode_config.clone()
    }
}
