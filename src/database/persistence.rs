//! Persistence Operations (Flush & Checkpoint)
//!
//! Extracted from database_legacy.rs
//! Handles data persistence and durability

use crate::database::core::MoteDB;
use crate::{Result, StorageError};
use std::sync::atomic::Ordering;

/// Get total size of all files in a directory (helper for checkpoint optimization)
fn get_directory_size(dir: &std::path::Path) -> Result<u64> {
    let mut total = 0;
    
    if !dir.exists() {
        return Ok(0);
    }
    
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_file() {
            total += metadata.len();
        }
    }
    
    Ok(total)
}

impl MoteDB {
    /// Flush database to disk
    /// 
    /// # Process
    /// 1. Rotate active MemTable to immutable queue
    /// 2. Trigger LSM flush (MemTable → SSTable)
    ///    - Automatically calls flush_callback
    ///    - Batch builds all indexes from MemTable
    /// 3. Persist other indexes (vector, spatial, text)
    /// 4. Reset pending counters
    /// 
    /// # Example
    /// ```ignore
    /// db.flush()?; // Persist all in-memory data
    /// ```
    pub fn flush(&self) -> Result<()> {
        ensure_open!(self);
        // 🔥 防止递归 flush (检查并设置标志)
        if self.is_flushing.compare_exchange(
            false, 
            true, 
            Ordering::Acquire, 
            Ordering::Relaxed
        ).is_err() {
            debug_log!("[MoteDB::flush] ⚠️  Skipped: Already flushing (防止递归)");
            return Ok(());
        }
        
        debug_log!("\n[MoteDB::flush] ========== START ==========");
        
        // 执行 flush，确保退出时重置标志
        let result = self.flush_impl();
        
        // 重置标志
        self.is_flushing.store(false, Ordering::Release);
        
        match &result {
            Ok(_) => {
                debug_log!("[MoteDB::flush] ========== DONE ✅ ==========\n");
            }
            Err(_e) => {
                debug_log!("[MoteDB::flush] ========== FAILED ❌ ==========");
                debug_log!("[MoteDB::flush] Error: {:?}\n", _e);
            }
        }
        
        result
    }
    
    /// Internal flush implementation
    fn flush_impl(&self) -> Result<()> {
        // 🔧 检查数据库路径是否存在（防止在删除后flush）
        if !self.path.exists() {
            debug_log!("⚠️  [flush] 数据库目录不存在，跳过flush: {:?}", self.path);
            return Ok(());
        }
        
        // 🔥 CRITICAL FIX: Rotate BEFORE scanning to avoid deadlock
        // 
        // Problem: scan_memtable_incremental_with() scans active + immutable,
        // holding memtable.read() lock, while background flush thread needs memtable.write() lock
        // causing deadlock!
        //
        // Solution: Force rotate first, then:
        // 1. Only scan immutable queue (using scan_immutable_only)
        // 2. Don't hold active MemTable lock, background thread can work normally
        
        // ✅ Correct Flush Process:
        // 1. force_rotate: Active → Immutable
        // 2. lsm_engine.flush(): Trigger real Flush
        //    ↓ During Flush, flush_callback is automatically called
        //    ↓ batch_build_indexes_from_flush() is triggered
        //    ↓ Column indexes are correctly built (directly from MemTable, no need to read SSTable)
        // 3. Other index persistence (Vector/Spatial/Text)
        
        // Step 1: Force rotate active MemTable to immutable queue
        self.lsm_engine.force_rotate()?;
        
        // Step 2: Flush (will trigger flush_callback → batch_build_indexes_from_flush)
        //         ✅ Indexes built in callback from MemTable (zero-copy, efficient)
        //         ✅ No need to read SSTable afterward (avoids timing issues)
        self.lsm_engine.flush()?;
        
        // Step 3: Persist other indexes to disk
        self.flush_vector_indexes()?;
        self.flush_spatial_indexes()?;
        self.flush_text_indexes()?;
        
        // 4. Reset pending counter
        // 🚀 P0 CRITICAL FIX: 使用原子操作
        self.pending_updates.store(0, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Checkpoint (flush WAL and indexes)
    /// 
    /// # Process (Optimized for Embedded)
    /// 1. Trigger LSM flush (MemTable → SSTable)
    /// 2. Rebuild timestamp index from LSM
    /// 3. Flush all indexes (persist to disk)
    /// 4. Checkpoint WAL (safe to truncate now)
    /// 
    /// # Optimizations
    /// - Skip WAL checkpoint if no changes (zero-cost)
    /// - Minimal memory allocation
    /// - Fast path for empty databases
    /// 
    /// # Example
    /// ```ignore
    /// db.checkpoint()?; // Full database checkpoint
    /// ```
    pub fn checkpoint(&self) -> Result<()> {
        ensure_open!(self);
        // 🔒 Prevent concurrent checkpoints (auto + manual can deadlock)
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

    /// Internal checkpoint implementation (caller must hold checkpoint_mutex)
    ///
    /// `rebuild_indexes`: if true, rebuild timestamp index from LSM (slow but thorough).
    ///                    If false, skip rebuild — rely on async pipeline for incremental updates.
    fn checkpoint_impl(&self, rebuild_indexes: bool) -> Result<()> {
        use std::time::Instant;

        // 🚀 Fast path: Skip if no pending updates and WAL is empty
        let pending_count = self.pending_updates.load(std::sync::atomic::Ordering::Relaxed);
        if pending_count == 0 {
            let wal_dir = self.path.join("wal");
            if let Ok(wal_size) = get_directory_size(&wal_dir) {
                if wal_size == 0 {
                    debug_log!("[Checkpoint] ⚡ Skip: No pending updates, WAL empty");
                    return Ok(());
                }
            }
        }

        let _checkpoint_start = Instant::now();
        debug_log!("[Checkpoint] 🚀 Starting {}checkpoint (pending_updates={})...",
            if rebuild_indexes { "full " } else { "" }, pending_count);

        // Step 1: Trigger LSM flush (MemTable → SSTable)
        let _flush_start = Instant::now();
        self.lsm_engine.flush()?;
        debug_log!("[Checkpoint]   ✓ LSM flush: {:?}", _flush_start.elapsed());

        // Step 2: Rebuild timestamp index (only in full checkpoint)
        if rebuild_indexes {
            let _ts_rebuild_start = Instant::now();
            self.rebuild_timestamp_index()?;
            debug_log!("[Checkpoint]   ✓ Timestamp rebuild: {:?}", _ts_rebuild_start.elapsed());
        }

        // Step 3: Flush all indexes (persist to disk)
        let _index_flush_start = Instant::now();
        self.flush_all_indexes()?;
        debug_log!("[Checkpoint]   ✓ Index flush: {:?}", _index_flush_start.elapsed());

        // Step 4: Checkpoint WAL (safe to truncate now)
        // CRITICAL: Only truncate WAL if immutable queue is empty.
        // If immutable queue has data (flush failed), WAL must be preserved for recovery.
        let immutable_queue_len = self.lsm_engine.immutable_queue_len();
        if immutable_queue_len == 0 {
            let _wal_checkpoint_start = Instant::now();
            self.wal.checkpoint_all()?;
            debug_log!("[Checkpoint]   ✓ WAL checkpoint: {:?}", _wal_checkpoint_start.elapsed());
        } else {
            debug_log!("[Checkpoint]   ⚠️ WAL NOT truncated: immutable queue has {} items (data safety)", immutable_queue_len);
        }

        // Step 5: Vacuum MVCC version store (remove old versions)
        let _vacuum_start = Instant::now();
        let current_ts = self.version_store.current_timestamp();
        match self.version_store.vacuum(current_ts) {
            Ok(removed) => {
                if removed > 0 {
                    debug_log!("[Checkpoint]   ✓ MVCC vacuum: removed {} old versions in {:?}", removed, _vacuum_start.elapsed());
                }
            }
            Err(e) => {
                debug_log!("[Checkpoint]   ⚠️ MVCC vacuum failed: {:?}", e);
            }
        }

        // Reset pending counters
        self.pending_updates.store(0, std::sync::atomic::Ordering::Relaxed);
        self.pending_spatial_updates.store(0, std::sync::atomic::Ordering::Relaxed);

        // Step 6: Persist AUTO_INCREMENT counters to catalog
        if let Err(e) = self.table_registry.persist_auto_increment_counters() {
            debug_log!("[Checkpoint]   ⚠️ Failed to persist AUTO_INCREMENT counters: {:?}", e);
        }

        debug_log!("[Checkpoint] 🎉 Total: {:?}", _checkpoint_start.elapsed());
        Ok(())
    }
    
    /// Flush all indexes (timestamp, vector, spatial, text, column)
    /// 
    /// ⚠️ **IMPORTANT**: This only flushes INDEX metadata, NOT MemTable data
    /// - For MemTable → Index migration, use `flush()` or `checkpoint()`
    /// - This is safe to call anytime (no data loss risk)
    pub fn flush_all_indexes(&self) -> Result<()> {
        // 1. Flush timestamp index
        self.timestamp_index.write().flush()?;
        
        // 2. Flush vector indexes
        self.flush_vector_indexes()?;
        
        // 3. Flush spatial indexes
        self.flush_spatial_indexes()?;
        
        // 4. Flush text indexes
        self.flush_text_indexes()?;
        
        // 5. Flush column indexes (先收集Arc，避免持锁期间flush)
        let indexes_to_flush: Vec<_> = self.column_indexes.iter()
            .map(|entry| entry.value().clone())
            .collect();
        
        for index in indexes_to_flush {
            index.write().flush()?;
        }
        
        Ok(())
    }
}
