//! Persistence Operations (Flush & Checkpoint)
//!
//! Extracted from database_legacy.rs
//! Handles data persistence and durability

use crate::database::core::MoteDB;
use crate::{Result, StorageError};
use std::sync::atomic::Ordering;
use std::sync::Arc;

/// Return freed heap memory to the OS after flush/checkpoint.
pub(crate) fn trim_allocator() {
    // jemalloc (default allocator when the feature is enabled): purge arenas.
    // This works on all platforms (macOS + Linux).
    crate::purge_memory_to_os();

    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn malloc_trim(__pad: usize) -> i32;
        }
        unsafe {
            malloc_trim(0);
        }
    }
    #[cfg(target_os = "macos")]
    {
        extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
        }
        unsafe {
            malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
        }
    }
}

impl MoteDB {
    /// Flush database to disk
    pub fn flush(&self) -> Result<()> {
        ensure_open!(self);
        if self
            .is_flushing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Ok(());
        }

        // Serialize with checkpoint_impl: the auto-checkpoint background thread
        // runs checkpoint_full concurrently, which calls sync_col_segment_to_
        // sstables → force_compact_all. If flush_impl's ColSegmentStore
        // flush_buffer runs concurrently with that compaction, segments can be
        // lost (the v0.5.0 large_batch data-loss bug — 10000 rows → 5000).
        let _ckpt_guard = self
            .checkpoint_mutex
            .lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()));

        let result = self.flush_impl();
        drop(_ckpt_guard);
        self.is_flushing.store(false, Ordering::Release);
        result
    }

    fn flush_impl(&self) -> Result<()> {
        if !self.path.exists() {
            return Ok(());
        }

        self.lsm_engine.force_rotate()?;
        self.lsm_engine.flush()?;

        // Only flush i-Octree here. Vector and text indexes are NOT flushed
        // because the async index-builder thread holds their write locks during
        // batch_insert. They are flushed during checkpoint_full() (Drop) after
        // the pipeline is stopped.
        self.flush_ioctree_indexes()?;

        if let Err(e) = self.columnar_store.flush_all() {
            debug_log!("[Flush] Columnar flush failed: {:?}", e);
        }

        // 🔥 Flush ColSegmentStore write buffers — without this, all buffered
        // INSERT/UPDATE/DELETE data is lost on restart. This was the root cause
        // of the "data disappears after reopen" bug found by durability tests.
        for entry in self.col_segment_stores.iter() {
            if let Err(e) = entry.flush_buffer() {
                debug_log!(
                    "[Flush] ColSegmentStore flush failed for {}: {:?}",
                    entry.key(),
                    e
                );
            }
            // Release mmap pages from flushed segments so RSS stays bounded.
            // Without this, each open segment's row_map + column data pages
            // remain resident, causing RSS to grow with segment count.
            entry.release_query_memory();
        }

        self.pending_updates.store(0, Ordering::Relaxed);
        trim_allocator();

        Ok(())
    }

    /// Checkpoint (flush WAL and indexes)
    pub fn checkpoint(&self) -> Result<()> {
        ensure_open!(self);
        let _guard = self
            .checkpoint_mutex
            .lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;
        self.checkpoint_impl(false)
    }

    /// Full checkpoint with index rebuild (used on shutdown/drop)
    pub fn checkpoint_full(&self) -> Result<()> {
        ensure_open!(self);
        let _guard = self
            .checkpoint_mutex
            .lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;
        self.checkpoint_impl(true)
    }

    /// VACUUM: force compaction and reclaim disk space.
    ///
    /// Flushes memtables, runs compaction on all LSM levels (dropping tombstones),
    /// then flushes and waits for all column indexes.
    pub fn vacuum(&self) -> Result<()> {
        ensure_open!(self);
        let _guard = self
            .checkpoint_mutex
            .lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;

        // Pause background compaction during vacuum.
        self.lsm_engine.pause_background_compaction();

        // 1. Flush all memtables to SSTables (background flush thread handles this)
        self.lsm_engine.flush()?;

        // Now pause flush thread too — all memtables are drained,
        // prevent new SSTables from appearing during compact_full.
        self.lsm_engine.pause_background_flush();

        // 2. Full compaction: merge ALL SSTables into a single file.
        //    Reduced from 3x to 1x: flush thread is paused above, one pass
        //    suffices. Saves ~3s on 500K-row workloads.
        if let Err(e) = self.lsm_engine.compact_full() {
            warn_log!("[VACUUM] Full compaction failed (non-fatal): {:?}", e);
        }

        // Resume background threads
        self.lsm_engine.resume_background_flush();
        self.lsm_engine.resume_background_compaction();

        // 3a. Finalize columnar write buffers → columnar SSTables.
        //     Accumulated INSERT data (zero-encode) is written to disk now.
        for entry in self.columnar_write_bufs.iter() {
            let table_name = entry.key().clone();
            let mut builder_guard = entry.value().lock();
            if builder_guard.num_rows > 0 {
                // Take the builder out, finish it, put a new empty one back
                let col_types = builder_guard.column_types.clone();
                let path = builder_guard.path.clone();
                let num_rows = builder_guard.num_rows;
                // Create a new empty builder to swap in
                let old_builder = std::mem::replace(
                    &mut *builder_guard,
                    crate::storage::lsm::columnar::ColumnarSSTableBuilder::new(&path, col_types),
                );
                drop(builder_guard);
                // Finish the old builder (writes to disk)
                if let Err(e) = old_builder.finish() {
                    warn_log!(
                        "[VACUUM] Failed to finalize columnar buffer for '{}': {:?}",
                        table_name,
                        e
                    );
                } else {
                    let indexes_dir = self.path.join("indexes");
                    let col_sst_path = indexes_dir.join(format!("{}_col.sst", &table_name));
                    if let Ok(col_sst) =
                        crate::storage::lsm::columnar::ColumnarSSTable::open(&col_sst_path)
                    {
                        self.columnar_sstables
                            .insert(table_name.clone(), Arc::new(col_sst));
                        debug_log!(
                            "[VACUUM] Columnar buffer finalized for '{}' ({} rows)",
                            table_name,
                            num_rows
                        );
                    }
                }
            }
        }

        // 3b. Columnar compaction: convert row-based SSTable → columnar for all tables.
        //    Non-fatal — if it fails, row-based scan still works.
        //    🆕 S9: skip ColSegmentStore tables (data is already in segment files,
        //    not the LSM — compact_to_columnar would be a no-op wasting time).
        for table_name in self.table_registry.list_tables()? {
            if self.col_segment_stores.contains_key(&table_name) {
                self.sync_col_segment_to_sstables(&table_name);
                continue;
            }
            if let Ok(schema) = self.table_registry.get_table(&table_name) {
                let col_types = schema.col_types();
                match self.lsm_engine.compact_to_columnar(col_types) {
                    Ok((col_sst, _source_paths)) => {
                        self.columnar_sstables
                            .insert(table_name.clone(), Arc::new(col_sst));
                        debug_log!(
                            "[VACUUM] Columnar SSTable created for table '{}'",
                            table_name
                        );
                    }
                    Err(e) => {
                        debug_log!(
                            "[VACUUM] Columnar compaction skipped for '{}': {:?}",
                            table_name,
                            e
                        );
                    }
                }
            }
        }

        // 4. Flush all column/text/vector indexes (non-fatal — core flush+compact is done)
        if let Err(e) = self.flush_all_indexes() {
            warn_log!("[VACUUM] Index flush failed (non-fatal): {}", e);
        }

        // 5. Clean up version store
        let min_active_ts = self.txn_coordinator.get_min_active_timestamp();
        if let Err(e) = self.version_store.vacuum(min_active_ts) {
            warn_log!("[VACUUM] Version store vacuum failed: {}", e);
        }

        // 6. Return freed memory to the OS (cross-platform)
        trim_allocator();

        Ok(())
    }

    /// Checkpoint during Drop — skips the is_closed check since we're shutting down.
    pub(crate) fn checkpoint_on_drop(&self) -> Result<()> {
        // 🔑 Flush ColSegmentStore write buffers so buffered data is durable.
        // WAL files remain on disk for crash recovery regardless, but flushing
        // buffers avoids replaying the entire WAL on next open.
        for entry in self.col_segment_stores.iter() {
            let _ = entry.flush_buffer();
        }
        Ok(())
    }

    fn checkpoint_impl(&self, rebuild_indexes: bool) -> Result<()> {
        // 🚀 Crash recovery: finalize columnar write buffers before checkpoint.
        //    Converts in-memory INSERT data to durable columnar SSTable files.
        //    On crash, at most one checkpoint interval of data is lost.
        for entry in self.columnar_write_bufs.iter() {
            let table_name = entry.key().clone();
            self.finalize_columnar_buffer(&table_name);
        }

        let pending_before = self.pending_updates.load(Ordering::Acquire);
        if pending_before == 0 {
            let wal_dir = self.path.join("wal");
            if let Ok(wal_size) = super::helpers::dir_size(&wal_dir) {
                if wal_size == 0 {
                    return Ok(());
                }
            }
        }

        self.lsm_engine.flush()?;

        if rebuild_indexes {
            self.rebuild_timestamp_index()?;
        }

        self.flush_all_indexes()?;

        // Re-check: if the LSM has pending immutable memtables, skip WAL
        // truncation (that data is only in the active memtable, not yet in an
        // SSTable). For ColSegmentStore tables (flushed above), the WAL data
        // is redundant and safe to truncate regardless.
        let immutable_queue_len = self.lsm_engine.immutable_queue_len();

        // 🔥 Flush ColSegmentStore write buffers BEFORE the WAL truncation
        // decision. This is critical for two reasons:
        // 1. ColSegmentStore is the source of truth (v0.3.0+). Once flush_buffer
        //    succeeds, the WAL data is redundant and can be safely truncated.
        // 2. Without this, the write_buf grows unboundedly (up to 100K rows =
        //    ~22MB heap per table) because the auto-checkpoint never flushes it.
        for entry in self.col_segment_stores.iter() {
            if let Err(e) = entry.flush_buffer() {
                debug_log!(
                    "[Flush] ColSegmentStore flush failed for {}: {:?}",
                    entry.key(),
                    e
                );
            }
            entry.release_query_memory();
        }

        if let Err(e) = self.columnar_store.flush_all() {
            warn_log!("[Flush] Columnar store flush failed: {}", e);
        }

        let checkpoint_done = if immutable_queue_len == 0 || !self.col_segment_stores.is_empty() {
            // All data has been flushed:
            // - LSM memtables are empty (immutable_queue is 0), OR
            // - ColSegmentStore tables are the source of truth and have been
            //   flushed above. For these tables, WAL records are redundant once
            //   the segment files are written. The WAL exists only for crash
            //   recovery of unflushed write_buf data.
            self.wal.checkpoint_all()?;
            // Persist write_lsn so restarts survive clock regression
            let current_lsn = self.write_lsn.load(std::sync::atomic::Ordering::SeqCst);
            crate::database::core::MoteDB::persist_lsn_counter(&self.path, current_lsn);
            true
        } else {
            false
        };

        let min_active_ts = self.txn_coordinator.get_min_active_timestamp();
        if let Err(e) = self.version_store.vacuum(min_active_ts) {
            warn_log!("[Flush] Version store vacuum failed: {}", e);
        }
        // Only reset pending_updates if WAL checkpoint was actually performed.
        // If skipped (new writes arrived during flush), keep the counter so
        // the next checkpoint knows there's outstanding data to flush.
        if checkpoint_done {
            self.pending_updates.store(0, Ordering::Relaxed);
        }

        // 🔥 Compact ColSegmentStore segments to reclaim disk and reduce segment
        // count. Without this, bulk INSERT creates many small segments (one per
        // flush) that stay on disk forever, growing linearly with data volume.
        // force_compact_all merges all segments into one, dropping tombstones
        // and old versions. This is the single most effective disk-reduction
        // operation for ColSegmentStore tables.
        for entry in self.col_segment_stores.iter() {
            if let Err(e) = entry.force_compact_all() {
                debug_log!(
                    "[Flush] ColSegmentStore compaction failed for {}: {:?}",
                    entry.key(),
                    e
                );
            }
            // Release pages after compaction (old segments are dropped, their
            // mmap pages should be returned to the OS).
            entry.release_query_memory();
        }

        if let Err(e) = self.table_registry.persist_auto_increment_counters() {
            warn_log!("[Flush] Auto-increment persistence failed: {}", e);
        }

        Ok(())
    }

    /// Flush all indexes (timestamp, vector, spatial, text, column)
    ///
    /// When the async index-builder pipeline is active, vector and text indexes
    /// are skipped because the builder thread holds their write locks.
    pub fn flush_all_indexes(&self) -> Result<()> {
        let async_pipeline = self.is_async_index_pipeline_active();

        self.timestamp_index.write().flush()?;

        if !async_pipeline {
            self.flush_vector_indexes()?;
        }

        if !async_pipeline {
            self.flush_text_indexes()?;
        }

        self.flush_ioctree_indexes()?;

        let indexes_to_flush: Vec<_> = self
            .column_indexes
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        for index in indexes_to_flush {
            index.flush()?;
        }

        Ok(())
    }
}
