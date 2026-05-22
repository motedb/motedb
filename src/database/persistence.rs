//! Persistence Operations (Flush & Checkpoint)
//!
//! Extracted from database_legacy.rs
//! Handles data persistence and durability

use crate::database::core::MoteDB;
use crate::{Result, StorageError};
use std::sync::atomic::Ordering;

/// Return freed heap memory to the OS after flush/checkpoint.
fn trim_allocator() {
    #[cfg(target_os = "linux")]
    {
        extern "C" {
            fn malloc_trim(__pad: usize) -> i32;
        }
        unsafe {
            malloc_trim(0);
        }
    }
}

impl MoteDB {
    /// Flush database to disk
    pub fn flush(&self) -> Result<()> {
        ensure_open!(self);
        if self.is_flushing.compare_exchange(
            false,
            true,
            Ordering::Acquire,
            Ordering::Relaxed
        ).is_err() {
            return Ok(());
        }

        let result = self.flush_impl();
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

        self.pending_updates.store(0, Ordering::Relaxed);
        trim_allocator();

        Ok(())
    }

    /// Checkpoint (flush WAL and indexes)
    pub fn checkpoint(&self) -> Result<()> {
        ensure_open!(self);
        let _guard = self.checkpoint_mutex.lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;
        self.checkpoint_impl(false)
    }

    /// Full checkpoint with index rebuild (used on shutdown/drop)
    pub fn checkpoint_full(&self) -> Result<()> {
        ensure_open!(self);
        let _guard = self.checkpoint_mutex.lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;
        self.checkpoint_impl(true)
    }

    /// VACUUM: force compaction and reclaim disk space.
    ///
    /// Flushes memtables, runs compaction on all LSM levels (dropping tombstones),
    /// then flushes and waits for all column indexes.
    pub fn vacuum(&self) -> Result<()> {
        ensure_open!(self);
        let _guard = self.checkpoint_mutex.lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;

        // 1. Flush all memtables to SSTables
        self.lsm_engine.flush()?;

        // 2. Run compaction on all levels (repeatedly until no more compaction needed)
        for _ in 0..10 {
            if !self.lsm_engine.compact()? {
                break;
            }
        }

        // 3. Flush all column/text/vector indexes
        self.flush_all_indexes()?;

        // 4. Clean up version store
        let min_active_ts = self.txn_coordinator.get_min_active_timestamp();
        if let Err(e) = self.version_store.vacuum(min_active_ts) {
            warn_log!("[VACUUM] Version store vacuum failed: {}", e);
        }

        Ok(())
    }

    /// Checkpoint during Drop — skips the is_closed check since we're shutting down.
    pub(crate) fn checkpoint_on_drop(&self) -> Result<()> {
        let _guard = self.checkpoint_mutex.lock()
            .map_err(|_| StorageError::Lock("Checkpoint mutex poisoned".into()))?;
        self.checkpoint_impl(true)
    }

    fn checkpoint_impl(&self, rebuild_indexes: bool) -> Result<()> {
        let pending_count = self.pending_updates.load(Ordering::Relaxed);
        if pending_count == 0 {
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

        let immutable_queue_len = self.lsm_engine.immutable_queue_len();
        if immutable_queue_len == 0 {
            self.wal.checkpoint_all()?;
        }

        let min_active_ts = self.txn_coordinator.get_min_active_timestamp();
        if let Err(e) = self.version_store.vacuum(min_active_ts) {
            warn_log!("[Flush] Version store vacuum failed: {}", e);
        }
        self.pending_updates.store(0, Ordering::Relaxed);
        if let Err(e) = self.columnar_store.flush_all() {
            warn_log!("[Flush] Columnar store flush failed: {}", e);
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

        let indexes_to_flush: Vec<_> = self.column_indexes.iter()
            .map(|entry| entry.value().clone())
            .collect();

        for index in indexes_to_flush {
            index.flush()?;
        }

        Ok(())
    }
}
