//! Persistence Operations (Flush & Checkpoint)
//!
//! Extracted from database_legacy.rs
//! Handles data persistence and durability

use crate::database::core::MoteDB;
use crate::{Result, StorageError};
use std::sync::atomic::Ordering;

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
        // 🔥 防止递归 flush (检查并设置标志)
        if self.is_flushing.compare_exchange(
            false, 
            true, 
            Ordering::Acquire, 
            Ordering::Relaxed
        ).is_err() {
            eprintln!("[MoteDB::flush] ⚠️  Skipped: Already flushing (防止递归)");
            return Ok(());
        }
        
        eprintln!("\n[MoteDB::flush] ========== START ==========");
        
        // 执行 flush，确保退出时重置标志
        let result = self.flush_impl();
        
        // 重置标志
        self.is_flushing.store(false, Ordering::Release);
        
        match &result {
            Ok(_) => {
                eprintln!("[MoteDB::flush] ========== DONE ✅ ==========\n");
            }
            Err(e) => {
                eprintln!("[MoteDB::flush] ========== FAILED ❌ ==========");
                eprintln!("[MoteDB::flush] Error: {:?}\n", e);
            }
        }
        
        result
    }
    
    /// Internal flush implementation
    fn flush_impl(&self) -> Result<()> {
        // 🔧 检查数据库路径是否存在（防止在删除后flush）
        if !self.path.exists() {
            eprintln!("⚠️  [flush] 数据库目录不存在，跳过flush: {:?}", self.path);
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
        *self.pending_updates.write() = 0;
        
        Ok(())
    }
    
    /// Checkpoint (flush WAL and indexes)
    /// 
    /// # Process
    /// 1. Trigger LSM flush (MemTable → SSTable)
    /// 2. Rebuild timestamp index from LSM
    /// 3. Flush all indexes (persist to disk)
    /// 4. Checkpoint WAL (safe to truncate now)
    /// 
    /// # Example
    /// ```ignore
    /// db.checkpoint()?; // Full database checkpoint
    /// ```
    pub fn checkpoint(&self) -> Result<()> {
        use std::time::Instant;
        let checkpoint_start = Instant::now();
        
        println!("[Checkpoint] 🚀 Starting batch index checkpoint...");
        
        // 🔥 Step 1: Trigger LSM flush (MemTable → SSTable)
        // This will also trigger batch index building via the callback
        let flush_start = Instant::now();
        self.lsm_engine.flush()?;
        println!("[Checkpoint]   ✓ LSM flush complete in {:?}", flush_start.elapsed());
        
        // 🔥 Step 2: Rebuild TimestampIndex from LSM (legacy path)
        // TODO: Move this to batch builder in future
        let ts_rebuild_start = Instant::now();
        self.rebuild_timestamp_index()?;
        println!("[Checkpoint]   ✓ Timestamp index rebuild in {:?}", ts_rebuild_start.elapsed());
        
        // 🔥 Step 3: Flush all indexes (persist to disk)
        let index_flush_start = Instant::now();
        self.flush_all_indexes()?;
        println!("[Checkpoint]   ✓ Index flush complete in {:?}", index_flush_start.elapsed());
        
        // 🔥 Step 4: Checkpoint WAL (safe to truncate now)
        let wal_checkpoint_start = Instant::now();
        self.wal.checkpoint_all()?;
        println!("[Checkpoint]   ✓ WAL checkpoint in {:?}", wal_checkpoint_start.elapsed());
        
        println!("[Checkpoint] 🎉 Total checkpoint time: {:?}", checkpoint_start.elapsed());
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
