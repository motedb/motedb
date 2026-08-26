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

    /// Online backup: copy a consistent point-in-time snapshot of the whole
    /// database directory to `dest` while the database stays open.
    ///
    /// Durability contract: every transaction COMMITted (durable) before the
    /// call is present in the snapshot. Concurrent autocommit writes are
    /// blocked for the duration of the copy (they queue on the write lock);
    /// in-flight explicit transactions are captured at their last durable
    /// state, exactly as a crash would see them.
    ///
    /// The snapshot is a plain directory copy — restore is simply
    /// `Database::open(dest)`. `dest` must not already exist.
    pub fn backup_to(&self, dest: &std::path::Path) -> Result<()> {
        ensure_open!(self);

        // Normalize exactly like open_with_config does (with_extension
        // "mote") so `Database::open(dest)` restores the snapshot from the
        // same path the user passed in.
        let dest = dest.with_extension("mote");

        if dest.exists() {
            return Err(StorageError::InvalidData(format!(
                "backup destination already exists: {}",
                dest.display()
            )));
        }

        // Same discipline as flush(): CAS the reentrancy flag, then hold
        // checkpoint_mutex so auto-checkpoint/compaction can't rewrite
        // segment files mid-copy.
        if self
            .is_flushing
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(StorageError::InvalidData(
                "a flush/checkpoint is in progress — retry backup".into(),
            ));
        }
        let _ckpt_guard = self
            .checkpoint_mutex
            .lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;

        // 🔑 Block autocommit writes FIRST (all stripes + global): records
        // acked in the window between flush_impl and checkpoint_all's
        // per-partition truncation lived only in volatile ColSegmentStore
        // write buffers (flush_impl had already passed them by) — their WAL
        // bytes got truncated with nothing durable left, and the snapshot
        // silently dropped them (BUG #35). With the barrier up front no
        // autocommit write can race the flush/truncate sequence. Writers
        // hold ONE stripe (per table) or the global write_lock (unparsed
        // fallback), so take ALL stripes in ascending order plus the global
        // lock — big-lock equivalence. Lock order checkpoint_mutex →
        // stripes → write_lock; writers take a single stripe and nothing
        // else, so no cycle.
        let mut _stripe_guards: Vec<parking_lot::MutexGuard<'_, ()>> =
            self.autocommit_locks.iter().map(|s| s.lock()).collect();
        let _write_guard = self.write_lock.lock();

        // Drain all in-memory write buffers to disk BEFORE copying, then
        // truncate the WAL: everything it still holds is now flushed into
        // segments and would otherwise be REPLAYED ON TOP of the flushed
        // data when the snapshot is opened (doubled TimeSeries rows).
        let flush_result = self.flush_impl().and_then(|()| {
            self.wal.checkpoint_all()?;
            Ok(())
        });

        let result = match flush_result {
            Ok(()) => Self::copy_dir_durable(&self.path, &dest),
            Err(e) => Err(e),
        };

        drop(_write_guard);
        drop(_ckpt_guard);
        self.is_flushing.store(false, Ordering::Release);
        result
    }

    /// Recursive directory copy with per-file fsync and a final directory
    /// fsync, so the completed copy survives power loss immediately (no
    /// reliance on dirty page cache). Skips the process lock file.
    fn copy_dir_durable(src: &std::path::Path, dest: &std::path::Path) -> Result<()> {
        std::fs::create_dir_all(dest).map_err(StorageError::Io)?;
        let entries = std::fs::read_dir(src).map_err(StorageError::Io)?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            // The flock file is process-local state, not database data.
            if name == ".lock" {
                continue;
            }
            let from = entry.path();
            let to = dest.join(&name);
            let meta = entry.metadata().map_err(StorageError::Io)?;
            if meta.is_dir() {
                Self::copy_dir_durable(&from, &to)?;
            } else {
                let mut in_file = std::fs::File::open(&from).map_err(StorageError::Io)?;
                let mut out_file = std::fs::File::create(&to).map_err(StorageError::Io)?;
                std::io::copy(&mut in_file, &mut out_file).map_err(StorageError::Io)?;
                out_file.sync_data().map_err(StorageError::Io)?;
            }
        }
        // Directory fsync makes the created entries themselves durable.
        let dir = std::fs::File::open(dest).map_err(StorageError::Io)?;
        dir.sync_all().map_err(StorageError::Io)?;
        Ok(())
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
        // 🔑 TTL retention enforcement rides along with checkpoints.
        self.enforce_ttls();
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

        // 🔑 4.5 Flush the remaining stores and TRUNCATE the WAL. Everything
        // the WAL still holds is now flushed into segments/columnar storage;
        // leaving it in place means a crash-recovery reopen REPLAYS it on top
        // of the flushed data — TimeSeries rows doubled (10 → 20) after
        // VACUUM + crash. Same root cause as the backup-snapshot doubling.
        {
            let _ = self.columnar_store.flush_all();
            for entry in self.col_segment_stores.iter() {
                let _ = entry.flush_buffer();
            }
            if let Err(e) = self.wal.checkpoint_all() {
                warn_log!("[VACUUM] WAL truncation failed (non-fatal): {}", e);
            }
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

        // 🔑 Flush indexes BEFORE the no-op early return below: an open that
        // only REBUILT indexes (crash recovery / corruption self-heal) has
        // pending posting lists but zero pending_updates and an empty WAL —
        // the early return would silently discard the rebuilt state.
        self.flush_all_indexes()?;

        let pending_before = self.pending_updates.load(Ordering::Acquire);
        if pending_before == 0 {
            let wal_dir = self.path.join("wal");
            if let Ok(wal_size) = super::helpers::dir_size(&wal_dir) {
                if wal_size == 0 {
                    return Ok(());
                }
            }
        }

        // 🚀 lsm.flush() 可能因后台线程卡住超时（累积效应）。ColSegmentStore 是
        // source of truth，LSM flush 失败不致命——数据已 WAL + ColSegmentStore。
        if let Err(e) = self.lsm_engine.flush() {
            warn_log!("[checkpoint] LSM flush failed (non-fatal): {}", e);
        }

        // 🔑 等 index-builder 处理完所有 pending batch 再碰索引。
        // LSM flush 上面触发了 flush callback，往 index-builder channel send 了
        // 新 batch。index-builder 处理 batch 时会 spawn 子线程（insert_batch 持
        // 索引写锁）。如果下面的 rebuild_timestamp_index / flush_all_indexes 在
        // 子线程持锁时获取索引锁，会死锁（CI 卡死根因）。
        // wait_for_indexes_ready 内部轮询 pending_index_batches，pending==0 时秒回。
        if self.has_pending_index_batches() {
            self.wait_for_indexes_ready_timeout(std::time::Duration::from_secs(10));
        }

        // 🔑 async pipeline 激活时跳过 rebuild_timestamp_index —— 它获取
        // timestamp_index 写锁，会和 index-builder 子线程竞争（同 flush_all_indexes）。
        if rebuild_indexes && !self.is_async_index_pipeline_active() {
            self.rebuild_timestamp_index()?;
        }

        // (indexes were flushed at the top of checkpoint_impl)

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
        // 🔒 Snapshot Arcs BEFORE looping — iter() holds shard read locks
        // while the loop body runs; force_compact_all can take seconds on
        // large tables, stalling every concurrent INSERT (entry() write on
        // this map) for the whole compaction. Snapshot-then-work keeps the
        // shard locks held only for the Arc clones.
        let stores: Vec<(String, Arc<crate::storage::col_segment::ColSegmentStore>)> = self
            .col_segment_stores
            .iter()
            .map(|e| (e.key().clone(), Arc::clone(e.value())))
            .collect();
        for (table_name, store) in stores {
            if let Err(e) = store.force_compact_all() {
                debug_log!(
                    "[Flush] ColSegmentStore compaction failed for {}: {:?}",
                    table_name,
                    e
                );
            }
            // Release pages after compaction (old segments are dropped, their
            // mmap pages should be returned to the OS).
            store.release_query_memory();
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

        // 🔑 async pipeline 激活时，所有索引由后台 index-builder 线程增量构建
        //（batch_build spawn 子线程，insert_batch 持索引内部写锁）。如果这里同时
        // flush 任何索引，都会和子线程竞争锁 → 死锁（close/checkpoint 卡死根因）。
        // async 模式下索引是可重建的派生数据，flush 多余（重启从数据重建），
        // 全部跳过。这一致地覆盖 timestamp/vector/text/ioctree/column 全部索引。
        if async_pipeline {
            return Ok(());
        }

        self.timestamp_index.write().flush()?;
        self.flush_vector_indexes()?;
        self.flush_text_indexes()?;
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
