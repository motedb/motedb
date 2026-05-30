//! LSM-Tree Engine (main interface)

use super::{UnifiedMemTable, SSTable, SSTableBuilder, Key, Value, ValueData, LSMConfig, CompactionWorker, BlobStore, BloomFilter};
use crate::{Result, StorageError};
use std::sync::{Arc, Mutex, Condvar};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::path::PathBuf;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::collections::VecDeque;

// Type aliases for complex types
type FlushCallback = Arc<dyn Fn(&UnifiedMemTable) -> Result<()> + Send + Sync>;
type KVIterator = Box<dyn Iterator<Item = Result<(Key, Value)>> + Send>;

/// Maximum consecutive flush errors before the circuit breaker trips.
/// Beyond this threshold the memtable is dropped (data remains in the WAL for recovery)
/// to prevent an infinite retry loop on permanent errors (e.g. disk full).
const MAX_CONSECUTIVE_FLUSH_ERRORS: u32 = 5;

/// Cached SSTable entry with separate bloom filter for lock-free pre-checking
struct CachedSSTable {
    bloom: Arc<BloomFilter>,
    handle: Arc<RwLock<SSTable>>,
}

/// LRU cache for SSTable handles with memory-aware eviction.
struct SSTableCache {
    cache: RwLock<lru::LruCache<PathBuf, CachedSSTable>>,
    max_entries: usize,
}

impl SSTableCache {
    fn new(max_size: usize) -> Self {
        use std::num::NonZeroUsize;
        Self {
            cache: RwLock::new(lru::LruCache::new(
                NonZeroUsize::new(max_size.max(1)).unwrap()
            )),
            max_entries: max_size,
        }
    }

    fn get_or_open(&self, path: &PathBuf) -> Result<CachedSSTable> {
        // Fast path: read lock + peek
        {
            let cache = self.cache.read();
            if let Some(cached) = cache.peek(path) {
                return Ok(CachedSSTable {
                    bloom: cached.bloom.clone(),
                    handle: cached.handle.clone(),
                });
            }
        }

        // Slow path: write lock
        let mut cache = self.cache.write();
        if let Some(cached) = cache.get(path) {
            return Ok(CachedSSTable {
                bloom: cached.bloom.clone(),
                handle: cached.handle.clone(),
            });
        }

        // Open new SSTable
        let sstable = SSTable::open(path)?;
        let bloom = Arc::new(sstable.bloom_filter().clone());
        let sstable_arc = Arc::new(RwLock::new(sstable));

        let cached = CachedSSTable {
            bloom: bloom.clone(),
            handle: sstable_arc.clone(),
        };

        // Evict old entries if at capacity
        while cache.len() >= self.max_entries {
            cache.pop_lru();
        }

        cache.put(path.clone(), CachedSSTable {
            bloom,
            handle: sstable_arc,
        });

        Ok(cached)
    }

    fn clear(&self) {
        self.cache.write().clear();
    }

    fn evict(&self, removed_paths: &[PathBuf]) {
        if removed_paths.is_empty() {
            return;
        }
        let mut cache = self.cache.write();
        for path in removed_paths {
            cache.pop(path);
        }
    }
}

/// LSM-Tree storage engine with multi-slot immutable queue
/// 
/// ## Architecture (🔥 NEW: Multi-slot Immutables)
/// - **Active MemTable**: Accepts writes (never blocks)
/// - **Immutable Queue**: 2 slots for flushing (buffered async)
/// - **Flush Thread**: Background thread that continuously flushes queue
/// 
/// ## Memory Control (🔥 Backpressure-enabled)
/// - Max memory: (1 + max_immutable_slots) × memtable_size = 5 × 4MB = 20MB
/// - When active is full: push to immutable queue, create new active
/// - Backpressure: If queue is full (2 slots occupied), wait for flush
/// - Benefit: Write throughput remains high even when disk is slow
/// 
/// ## Performance
/// - Fast path: No backpressure, ~1μs per write
/// - Slow disk: Up to 4 × memtable_size buffered (16MB), prevents OOM
/// - Flush rate: Limited by disk fsync speed (~100 ops/sec on macOS)
/// 
/// ## Thread Management (🔧 Optimized for graceful shutdown)
/// - Background threads hold `Weak` references (not `Arc`)
/// - Drop() signals shutdown and waits for threads to exit
/// - No Arc cycle, memory released immediately on drop
/// 
/// ## 🆕 Phase 1 Part 2: Unified MemTable Integration
/// - 支持数据 + 向量的统一存储
/// - `UnifiedMemTable` 集成 `FreshVamanaGraph`
/// - 向量搜索直接返回完整 row data
pub struct LSMEngine {
    /// Active MemTable (accepting writes)
    /// 🆕 现在使用 UnifiedMemTable（支持数据+向量）
    memtable: Arc<RwLock<UnifiedMemTable>>,
    
    /// Immutable MemTable queue (FIFO, up to 2 slots)
    /// 🔥 NEW: Changed from Option to VecDeque for multi-slot buffering
    immutable: Arc<RwLock<VecDeque<UnifiedMemTable>>>,
    
    /// Maximum immutable slots (default: 4)
    max_immutable_slots: usize,
    
    /// Flush lock (prevents concurrent flush operations)
    flush_lock: Arc<Mutex<()>>,
    
    /// Flush in progress flag (atomic, lock-free check)
    flush_in_progress: Arc<AtomicBool>,
    
    /// Shutdown signal for background threads
    shutdown: Arc<AtomicBool>,
    
    /// SSTable cache (减少文件打开开销)
    sstable_cache: Arc<SSTableCache>,
    
    /// Storage directory
    storage_dir: PathBuf,
    
    /// Configuration
    config: LSMConfig,
    
    /// Next SSTable ID
    next_sst_id: Arc<RwLock<u64>>,
    
    /// Compaction worker
    compaction_worker: Arc<CompactionWorker>,
    
    /// Blob store for large values
    blob_store: Arc<BlobStore>,
    
    /// 🔧 Background thread handles (for graceful shutdown)
    compaction_thread: Option<JoinHandle<()>>,
    flush_thread: Option<JoinHandle<()>>,

    /// 🚀 Edge optimization: Condvar for event-driven flush (replaces 10ms polling)
    flush_wakeup: Arc<(Mutex<bool>, Condvar)>,

    /// 🚀 Edge optimization: Condvar for event-driven compaction (replaces 500ms polling)
    compaction_wakeup: Arc<(Mutex<bool>, Condvar)>,

    /// 🚀 Unified Flush Callback
    /// Callback: &UnifiedMemTable -> Result<()>
    /// Called during flush to enable batch index building
    ///
    /// ✅ 统一入口：手动Flush和后台Flush都会触发
    /// ✅ 传入MemTable引用：避免数据拷贝，高效批量构建
    flush_callback: Arc<RwLock<Option<FlushCallback>>>,

    /// Circuit breaker: counts consecutive SSTable write failures.
    /// After exceeding MAX_CONSECUTIVE_FLUSH_ERRORS, the flush thread drops the
    /// memtable instead of requeueing it (data is still in WAL for recovery).

    /// Monotonically increasing epoch, bumped on every memtable rotation.
    /// Readers load it before+after scanning active/immutable to detect
    /// concurrent rotations and retry if needed (lock-free consistency).
    rotation_epoch: Arc<AtomicU64>,
    /// Reset to 0 on any successful flush.
    consecutive_flush_errors: Arc<std::sync::atomic::AtomicU32>,
}

impl LSMEngine {
    /// Create a new LSM engine (without vector support)
    pub fn new(storage_dir: PathBuf, config: LSMConfig) -> Result<Self> {
        Self::new_internal(storage_dir, config, None)
    }
    
    /// 🆕 Create a new LSM engine with vector support
    /// 
    /// ## Parameters
    /// - `storage_dir`: 存储目录
    /// - `config`: LSM 配置
    /// - `vector_dimension`: 向量维度（例如 128, 384, 768）
    /// 
    /// ## Example
    /// ```ignore
    /// let engine = LSMEngine::new_with_vector_support(
    ///     PathBuf::from("/tmp/db"),
    ///     LSMConfig::default(),
    ///     768  // 向量维度
    /// )?;
    /// ```ignore
    pub fn new_with_vector_support(storage_dir: PathBuf, config: LSMConfig, vector_dimension: usize) -> Result<Self> {
        Self::new_internal(storage_dir, config, Some(vector_dimension))
    }
    
    /// Internal constructor (统一初始化逻辑)
    fn new_internal(storage_dir: PathBuf, config: LSMConfig, vector_dimension: Option<usize>) -> Result<Self> {
        std::fs::create_dir_all(&storage_dir)?;

        // Clean up leftover .sst.tmp files from interrupted flushes
        if let Ok(entries) = std::fs::read_dir(&storage_dir) {
            for entry in entries.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    if name.ends_with(".sst.tmp") {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }

        let compaction_worker = Arc::new(CompactionWorker::new(storage_dir.clone(), &config));

        // Clean up orphan .sst files — files on disk not in the compaction worker's
        // level metadata. These can be left behind by interrupted compaction or flush.
        // Safety: move unreadable files to lost+found instead of deleting them,
        // because a truncated footer does not mean the data is unrecoverable.
        // Also clean up old lost+found files to prevent unbounded disk growth.
        {
            let lost_found = storage_dir.join("lost+found");
            if lost_found.exists() {
                if let Ok(entries) = std::fs::read_dir(&lost_found) {
                    for entry in entries.flatten() {
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        {
            let known_paths: std::collections::HashSet<PathBuf> = compaction_worker
                .get_all_sstables()
                .map(|metas| metas.iter().map(|m| m.path.clone()).collect())
                .unwrap_or_default();

            if let Ok(entries) = std::fs::read_dir(&storage_dir) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().and_then(|e| e.to_str()) == Some("sst")
                        && !known_paths.contains(&path)
                    {
                        debug_log!("[LSM] Moving orphan SSTable to lost+found: {:?}", path);
                        let lost_found = storage_dir.join("lost+found");
                        let _ = std::fs::create_dir_all(&lost_found);
                        let dest = lost_found.join(path.file_name().unwrap_or_default());
                        let _ = std::fs::rename(&path, &dest);
                    }
                }
            }
        }

        // Initialize blob store
        let blob_dir = storage_dir.join("blobs");
        let blob_store = Arc::new(BlobStore::new(blob_dir, config.blob_file_size)?);
        
        // 🆕 Create UnifiedMemTable (with or without vector support)
        let memtable = if let Some(dim) = vector_dimension {
            UnifiedMemTable::new_with_vector_support(&config, dim)
        } else {
            UnifiedMemTable::new(&config)
        };

        // Recover next_sst_id from existing SSTables to avoid overwriting on restart.
        // Scan ALL levels (l0_, l1_, l2_, ...) since compaction output files also
        // consume IDs and can collide with flush output if not accounted for.
        let max_existing_id = compaction_worker
            .get_all_sstables()
            .map(|metas| {
                metas.iter()
                    .filter_map(|m| {
                        let stem = m.path.file_stem()?.to_str()?;
                        // Strip any "lN_" prefix (l0_, l1_, l2_, etc.)
                        let id_str = stem.split('_').next_back()?;
                        id_str.parse::<u64>().ok()
                    })
                    .max()
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        let mut engine = Self {
            memtable: Arc::new(RwLock::new(memtable)),
            immutable: Arc::new(RwLock::new(VecDeque::new())),  // 🔥 Empty queue
            max_immutable_slots: 2,  // 2 slots = 8MB peak buffer (embedded-friendly)
            flush_lock: Arc::new(Mutex::new(())),
            flush_in_progress: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            sstable_cache: Arc::new(SSTableCache::new(config.sstable_cache_size)),
            storage_dir,
            config: config.clone(),
            next_sst_id: Arc::new(RwLock::new(max_existing_id + 1)),
            compaction_worker: compaction_worker.clone(),
            blob_store,
            compaction_thread: None,
            flush_thread: None,
            flush_callback: Arc::new(RwLock::new(None)),
            rotation_epoch: Arc::new(AtomicU64::new(0)),
            flush_wakeup: Arc::new((Mutex::new(false), Condvar::new())),
            compaction_wakeup: Arc::new((Mutex::new(false), Condvar::new())),
            consecutive_flush_errors: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        };

        // Wire post-compaction callback to evict only removed SSTables from cache
        {
            let cache = engine.sstable_cache.clone();
            engine.compaction_worker.set_post_compaction_cb(Box::new(move |removed_paths: &[PathBuf]| {
                cache.evict(removed_paths);
            }));
        }

        // 🔥 Start background compaction thread with Weak references
        let compaction_worker_weak = Arc::downgrade(&engine.compaction_worker);
        let shutdown_weak = Arc::downgrade(&engine.shutdown);
        let compaction_wakeup = engine.compaction_wakeup.clone();
        
        let compaction_thread = thread::spawn(move || {
            let mut _consecutive_no_work = 0;

            while let Some(shutdown) = shutdown_weak.upgrade() {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                {
                    let (lock, cvar) = &*compaction_wakeup;
                    let guard = lock.lock().unwrap();
                    let _ = cvar.wait_timeout(guard, Duration::from_secs(30));
                }

                let compaction_worker = match compaction_worker_weak.upgrade() {
                    Some(w) => w,
                    None => break,
                };

                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    match compaction_worker.needs_compaction() {
                        Ok(true) => {
                            let mut rounds = 0;
                            while let Ok(true) = compaction_worker.needs_compaction() {
                                if let Err(e) = compaction_worker.run_compaction() {
                                    debug_log!("Compaction error: {:?}", e);
                                    break;
                                }
                                rounds += 1;
                                if rounds > 10 { break; }
                            }
                            true // had work
                        }
                        Ok(false) => false, // no work
                        Err(e) => { debug_log!("Compaction check error: {:?}", e); false }
                    }
                }));

                match result {
                    Ok(true) => _consecutive_no_work = 0,
                    Ok(false) => _consecutive_no_work += 1,
                    Err(_) => {
                        eprintln!("[MoteDB] Compaction thread panicked, restarting...");
                        std::thread::sleep(Duration::from_secs(1));
                    }
                }
            }
        });
        
        // 🔥 Start background flush thread with Weak references
        let immutable_weak = Arc::downgrade(&engine.immutable);
        let flush_in_progress_weak = Arc::downgrade(&engine.flush_in_progress);
        let shutdown_weak = Arc::downgrade(&engine.shutdown);
        let storage_dir_clone = engine.storage_dir.clone();
        let config_clone = engine.config.clone();
        let next_sst_id_weak = Arc::downgrade(&engine.next_sst_id);
        let compaction_worker_weak = Arc::downgrade(&engine.compaction_worker);
        let flush_callback_weak = Arc::downgrade(&engine.flush_callback); // 🔥 NEW: Callback for index building
        let flush_wakeup = engine.flush_wakeup.clone(); // 🚀 Condvar for event-driven flush
        let compaction_wakeup_for_flush = engine.compaction_wakeup.clone(); // Notify compaction after SST build
        let consecutive_flush_errors = engine.consecutive_flush_errors.clone(); // Circuit breaker

        let flush_thread = thread::Builder::new()
            .name("lsm-flush".to_string())
            .spawn(move || {
            loop {
                // Wrap each iteration in catch_unwind so the thread survives panics
                let iter_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                // 🔧 Check shutdown signal
                let shutdown = match shutdown_weak.upgrade() {
                    Some(s) => s,
                    None => {
                        return false; // signal: break loop
                    }
                };

                if shutdown.load(Ordering::Relaxed) {
                    return false; // signal: break loop
                }

                // Quick lock-free check
                let flush_in_progress = match flush_in_progress_weak.upgrade() {
                    Some(f) => f,
                    None => return false,
                };

                let fip = flush_in_progress.load(Ordering::Acquire);
                if !fip {
                    let immutable = match immutable_weak.upgrade() {
                        Some(i) => i,
                        None => return false,
                    };

                    let has_immutable = {
                        let immutable_guard = immutable.read();
                        !immutable_guard.is_empty()
                    };

                    if has_immutable {
                        // Try to flush (inline implementation to avoid circular reference)
                        if flush_in_progress.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
                            // 🔒 Drop guard: ensures flush_in_progress is reset even on panic
                            struct FlushGuard {
                                flag: Arc<AtomicBool>,
                            }
                            impl Drop for FlushGuard {
                                fn drop(&mut self) {
                                    self.flag.store(false, Ordering::Release);
                                }
                            }
                            let _flush_guard = FlushGuard { flag: flush_in_progress.clone() };

                            // 🔥 CORRECTNESS FIX: Peek at front memtable WITHOUT popping.
                            let memtable_len = {
                                let immutable_guard = immutable.read();
                                match immutable_guard.front() {
                                    Some(mt) => mt.len(),
                                    None => 0,
                                }
                            };

                            if memtable_len == 0 {
                                // Empty memtable in queue — just pop it to avoid infinite loop
                                let mut immutable_lock = immutable.write();
                                immutable_lock.pop_front();
                            } else {
                                let next_sst_id = match next_sst_id_weak.upgrade() {
                                    Some(n) => n,
                                    None => {
                                        return false;
                                    }
                                };

                                let sst_id = {
                                    let mut next_id = next_sst_id.write();
                                    let current = *next_id;
                                    *next_id += 1;
                                    Some(current)
                                };

                                if let Some(sst_id) = sst_id {
                                    let sst_path = storage_dir_clone.join(format!("l0_{:06}.sst", sst_id));

                                    // 🔧 Ensure storage directory exists
                                    if !storage_dir_clone.exists() {
                                        debug_log!("[LSM Flush] ⚠️  Storage directory deleted, skipping flush");
                                        return false;
                                    }

                                    // Build SSTable with retry on failure (data loss prevention)
                                    let mut flush_success = false;
                                    for attempt in 0..3 {
                                        match SSTableBuilder::new(&sst_path, config_clone.clone(), memtable_len) {
                                            Ok(mut builder) => {
                                                let immutable_guard = immutable.read();
                                                let mut add_failed = false;
                                                if let Some(front_mt) = immutable_guard.front() {
                                                    for (key, entry) in front_mt.iter() {
                                                        let value = Value {
                                                            data: entry.data,
                                                            timestamp: entry.timestamp,
                                                            deleted: entry.deleted,
                                                        };
                                                        if let Err(e) = builder.add(key, value) {
                                                            debug_log!("[LSM Flush] Error adding key {}: {:?}", key, e);
                                                            add_failed = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                drop(immutable_guard);
                                                if add_failed {
                                                    continue; // retry this attempt
                                                }

                                                match builder.finish() {
                                                    Ok(meta) => {
                                                        if let Some(worker) = compaction_worker_weak.upgrade() {
                                                            if let Err(e) = worker.register_sstable(meta) {
                                                                debug_log!("[LSM Flush] ❌ CRITICAL: register_sstable failed: {:?}. SSTable on disk but not tracked.", e);
                                                                continue;
                                                            }
                                                        }
                                                        // 🚀 Wake compaction thread (new SSTable registered)
                                                        {
                                                            let (lock, cvar) = &*compaction_wakeup_for_flush;
                                                            if let Ok(mut guard) = lock.lock() { *guard = true; }
                                                            cvar.notify_all();
                                                        }
                                                        flush_success = true;
                                                        break;
                                                    }
                                                    Err(e) => {
                                                        debug_log!("[LSM Flush] ❌ Failed to finish SSTable_{} (attempt {}): {:?}", sst_id, attempt + 1, e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                debug_log!("[LSM Flush] ❌ Failed to create SSTable builder (attempt {}): {:?}", attempt + 1, e);
                                            }
                                        }
                                        // Wait before retry
                                        if attempt < 2 {
                                            std::thread::sleep(Duration::from_millis(100 * (attempt as u64 + 1)));
                                        }
                                    }

                                    if flush_success {
                                        // Reset circuit breaker on successful flush
                                        consecutive_flush_errors.store(0, Ordering::Relaxed);

                                        // SSTable is registered — now safe to pop (data visible via SSTable)
                                        let memtable = {
                                            let mut immutable_lock = immutable.write();
                                            immutable_lock.pop_front()
                                        };

                                        // 🔥 Call flush callback with owned memtable
                                        if let Some(memtable) = memtable {
                                            if let Some(callback_arc) = flush_callback_weak.upgrade() {
                                                let callback_guard = callback_arc.read();
                                                if let Some(ref callback) = *callback_guard {
                                                    if let Err(e) = callback(&memtable) {
                                                        debug_log!("[LSM Flush] ⚠️  Callback error: {:?}", e);
                                                    }
                                                }
                                            }
                                            drop(memtable);
                                        }
                                    } else {
                                        // Circuit breaker: track errors but never drop the memtable.
                                        // Dropping is unsafe when WAL is not active (permanent data loss).
                                        // Instead, log a critical warning and retry on the next cycle.
                                        let errors = consecutive_flush_errors.fetch_add(1, Ordering::Relaxed) + 1;
                                        if errors > MAX_CONSECUTIVE_FLUSH_ERRORS {
                                            debug_log!(
                                                "[LSM Flush] 🚨 CRITICAL: {} consecutive flush failures. Retrying — memtable NOT dropped to prevent data loss.",
                                                errors
                                            );
                                            // Back off to reduce log spam and disk pressure
                                            std::thread::sleep(std::time::Duration::from_secs(1));
                                        }
                                    }
                                }
                            } // end else (memtable_len > 0)

                            // _flush_guard Drop resets flush_in_progress=false

                            // Notify anyone waiting in flush() that the immutable
                            // queue has been drained.
                            {
                                let (lock, cvar) = &*flush_wakeup;
                                if let Ok(mut guard) = lock.lock() {
                                    *guard = true;
                                }
                                cvar.notify_all();
                            }
                        }
                    }
                }

                // Wait for new work: wake on notification or poll every 500ms.
                // Check the flag AFTER acquiring the lock to avoid clearing a
                // signal that arrived between the work check and this wait.
                {
                    let (lock, cvar) = &*flush_wakeup;
                    let mut guard = lock.lock().unwrap();
                    // Only clear+wait if no new signal arrived while we were working
                    if !*guard {
                        *guard = false;
                        let _ = cvar.wait_timeout(guard, Duration::from_millis(500));
                    } else {
                        // Signal arrived while we were processing — loop immediately
                        *guard = false;
                    }
                }
                true // signal: continue loop
                }));  // end catch_unwind for this iteration

                match iter_result {
                    Ok(should_continue) => {
                        if !should_continue {
                            break;
                        }
                    }
                    Err(panic_payload) => {
                        debug_log!("[LSM Flush Thread] ❌ PANIC in iteration: {:?}", panic_payload);
                        // Thread survives — continue to next iteration
                        // Brief sleep to avoid tight panic loop
                        thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        }).map_err(|e| StorageError::Io(std::io::Error::other(format!("Failed to spawn flush thread: {}", e))))?;
        
        engine.compaction_thread = Some(compaction_thread);
        engine.flush_thread = Some(flush_thread);
        
        Ok(engine)
    }
    
    /// Put a key-value pair (WITH BACKPRESSURE to prevent OOM)
    /// 
    /// ## Multi-slot Immutable Queue Architecture
    /// 1. Insert into active MemTable
    /// 2. If active is full: push to immutable queue, create new active
    /// 3. Background thread flushes queue continuously (FIFO)
    /// 
    /// ## Memory Control (🔥 NEW: Queue-based Backpressure)
    /// - Max memory: (1 + max_slots) × memtable_size = 5 × 4MB = 20MB
    /// - If queue has space (< 2 slots): No blocking, instant rotation
    /// - If queue is full (= 2 slots): **Block write until a slot frees**
    /// - Benefit: Smooth writes even when disk fsync is slow
    /// 
    /// ## Performance
    /// - Fast path: ~1μs per write (no backpressure)
    /// - Slow disk: Up to 16MB buffered, ~10ms wait max
    /// - Memory bounded: Guaranteed ≤ 20MB
    pub fn put(&self, key: Key, mut value: Value) -> Result<()> {
        // Check if value should go to blob storage
        if let ValueData::Inline(ref data) = value.data {
            if data.len() >= self.config.blob_threshold {
                // Move large value to blob store
                let blob_ref = self.blob_store.put(data)?;
                value.data = ValueData::Blob(blob_ref);
            }
        }

        // 🔥 BACKPRESSURE: Wait if active is full AND queue is at max capacity
        let mut backpressure_count = 0;
        loop {
            // 🚀 Phase 3.1: Combine flush check + insert into single lock acquisition
            // Fast path: acquire read lock, check flush, and insert in one go
            {
                let memtable = self.memtable.read();

                if !memtable.should_flush() {
                    // Fast path: active has space, insert while holding the lock
                    memtable.put(key, value)?;
                    return Ok(());
                }
                // Slow path: memtable is full, drop lock and handle rotation
            }

            // Check queue capacity before rotating
            let queue_len = {
                let immutable = self.immutable.read();
                immutable.len()
            };

            if queue_len < self.max_immutable_slots {
                // Queue has space, try to rotate
                if self.try_rotate_memtable().is_ok() {
                    // Retry the insert after rotation (new active memtable has space)
                    continue;
                }
            }

            // Queue is full, apply backpressure
            backpressure_count += 1;
            if backpressure_count == 1 {
                debug_log!("[LSM] ⚠️  Backpressure: Queue full ({}/{}), waiting for flush...",
                    queue_len, self.max_immutable_slots);
            } else if backpressure_count % 100 == 0 {
                debug_log!("[LSM] ⏳ Still waiting: {}ms (queue: {}/{})",
                    backpressure_count * 10, queue_len, self.max_immutable_slots);
            }

            thread::sleep(Duration::from_millis(10));

            // Safety: prevent infinite loop (100 seconds timeout)
            if backpressure_count > 10000 {
                return Err(StorageError::Transaction(
                    "LSM backpressure timeout: flush thread may be deadlocked".into()
                ));
            }
        }
    }
    
    /// Get a value by key (LSM查询: MemTable -> Immutable -> SSTables -> Blob)
    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        // 1. Check active memtable (newest data)
        let epoch_before = self.rotation_epoch.load(Ordering::Acquire);
        let active_result = {
            let memtable = self.memtable.read();
            memtable.get(key)?
        };

        if let Some(entry) = active_result {
            let mut value = Value {
                data: entry.data,
                timestamp: entry.timestamp,
                deleted: entry.deleted,
            };

            if value.deleted {
                return Ok(None);
            }

            if let ValueData::Blob(ref blob_ref) = value.data {
                let blob_data = self.blob_store.get(blob_ref)?;
                value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
            }
            return Ok(Some(value));
        }

        // 2. Check immutable queue (reverse order, newer first)
        let immutable_result = {
            let immutable = self.immutable.read();

            let mut result = None;
            for memtable in immutable.iter().rev() {
                if let Some(entry) = memtable.get(key)? {
                    result = Some(entry);
                    break;
                }
            }
            result
        };

        // 2b. Epoch check: if a rotation happened, the old active (which didn't
        //     have our key) may now be in immutable. Re-check immutable.
        let found_in_immutable = immutable_result.is_some();
        let epoch_after = self.rotation_epoch.load(Ordering::Acquire);
        let immutable_result = if !found_in_immutable && epoch_after != epoch_before {
            let immutable = self.immutable.read();
            let mut result = None;
            for memtable in immutable.iter().rev() {
                if let Some(entry) = memtable.get(key)? {
                    result = Some(entry);
                    break;
                }
            }
            result
        } else {
            immutable_result
        };

        if let Some(entry) = immutable_result {
            // 🆕 Convert UnifiedEntry → Value
            let mut value = Value {
                data: entry.data,
                timestamp: entry.timestamp,
                deleted: entry.deleted,
            };
            
            // Check tombstone (DELETE 操作)
            if value.deleted {
                return Ok(None);
            }
            
            // Resolve blob reference
            if let ValueData::Blob(ref blob_ref) = value.data {
                let blob_data = self.blob_store.get(blob_ref)?;
                value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
            }
            return Ok(Some(value));
        }
        
        // 3. Check SSTables (Level 0 -> Level 1 -> ... -> Level N)
        //    When the same key exists in multiple SSTables (e.g. after compaction
        //    where old L0 files haven't been deleted yet), return the version with
        //    the highest timestamp (newest).
        let sstable_metas = self.compaction_worker.get_all_sstables()?;
        let mut best: Option<Value> = None;

        for level in 0..self.config.num_levels {
            for meta in sstable_metas.iter()
                .filter(|meta| self.get_level_from_path(&meta.path) == level)
                .rev()
            {
                // Quick check: key in range? [min_key, max_key] inclusive
                if key < meta.min_key || key > meta.max_key {
                    continue;
                }

                // 🚀 Lock-free bloom filter pre-check from SSTableMeta
                if let Some(ref bloom) = meta.bloom_filter {
                    if !bloom.may_contain(&key.to_be_bytes()) {
                        continue;
                    }
                }

                let cached = match self.sstable_cache.get_or_open(&meta.path) {
                    Ok(cached) => cached,
                    Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        continue;
                    }
                    Err(e) => return Err(e),
                };

                if meta.bloom_filter.is_none() && !cached.bloom.may_contain(&key.to_be_bytes()) {
                    continue;
                }

                let sstable = cached.handle.read();

                if let Some(mut value) = sstable.get(key)? {
                    if let ValueData::Blob(ref blob_ref) = value.data {
                        let blob_data = self.blob_store.get(blob_ref)?;
                        value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
                    }

                    // Keep the version with the highest timestamp
                    if best.as_ref().is_none_or(|b| value.timestamp > b.timestamp) {
                        best = Some(value);
                    }
                }
            }

            // Early exit: if we found a non-tombstone entry at this level,
            // no need to check deeper levels (they have older data).
            // But we DO need to finish scanning all SSTables at the SAME level
            // because L0 SSTables are unordered and can overlap.
            if best.is_some() && level > 0 {
                break;
            }
        }

        if let Some(value) = best {
            if value.deleted {
                return Ok(None);
            }
            return Ok(Some(value));
        }
        
        Ok(None)
    }
    
    /// 🚀 Batch get (避免在循环中反复获取锁)
    /// 
    /// **关键优化**：
    /// - 一次性获取immutable.read()锁，查询所有keys
    /// - 减少锁竞争：N次get() → 1次batch_get()
    /// - 避免读者饥饿：减少与flush线程的锁竞争
    pub fn batch_get(&self, keys: &[Key]) -> Result<Vec<Option<Value>>> {
        debug_log!("[batch_get] batch query {} keys", keys.len());
        let mut results = vec![None; keys.len()];
        let mut remaining_keys: Vec<(usize, Key)> = keys.iter().enumerate().map(|(i, &k)| (i, k)).collect();

        // Capture epoch for re-check after immutable scan
        let epoch_before = self.rotation_epoch.load(Ordering::Acquire);

        // 1. Check active memtable
        {
            let memtable = self.memtable.read();

            let mut i = 0;
            while i < remaining_keys.len() {
                let (idx, key) = remaining_keys[i];
                if let Some(entry) = memtable.get(key)? {
                    let mut value = Value {
                        data: entry.data,
                        timestamp: entry.timestamp,
                        deleted: entry.deleted,
                    };
                    
                    // Resolve blob reference if needed
                    if let ValueData::Blob(ref blob_ref) = value.data {
                        let blob_data = self.blob_store.get(blob_ref)?;
                        value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
                    }
                    
                    // Don't return tombstones (keep as None for deleted entries)
                    if !value.deleted {
                        results[idx] = Some(value);
                    }
                    remaining_keys.swap_remove(i);
                } else {
                    i += 1;
                }
            }
            // memtable lock released here
        }
        
        if remaining_keys.is_empty() {
            debug_log!("✅ [batch_get] 所有keys在active memtable中找到，直接返回");
            return Ok(results);
        }
        
        // 2. Check immutable queue (批量查询，只获取一次锁)
        {
            let immutable = self.immutable.read();

            for (mt_idx, memtable) in immutable.iter().rev().enumerate() {
                debug_log!("  🔍 [batch_get] 查询第 {} 个immutable memtable，剩余 {} 个keys", mt_idx + 1, remaining_keys.len());
                let mut i = 0;
                while i < remaining_keys.len() {
                    let (idx, key) = remaining_keys[i];
                    if let Some(entry) = memtable.get(key)? {
                        let mut value = Value {
                            data: entry.data,
                            timestamp: entry.timestamp,
                            deleted: entry.deleted,
                        };
                        
                        // Resolve blob reference
                        if let ValueData::Blob(ref blob_ref) = value.data {
                            let blob_data = self.blob_store.get(blob_ref)?;
                            value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
                        }
                        
                        // Don't return tombstones (keep as None for deleted entries)
                        if !value.deleted {
                            results[idx] = Some(value);
                        }
                        remaining_keys.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                
                if remaining_keys.is_empty() {
                    debug_log!("  ✅ [batch_get] 所有keys已找到，提前退出immutable查询");
                    break;
                }
            }
            // immutable lock released here
        }

        // Re-check epoch: if a rotation happened between active and immutable scans,
        // re-scan immutable queue for any remaining keys that rotated in.
        let epoch_after = self.rotation_epoch.load(Ordering::Acquire);
        if epoch_after != epoch_before && !remaining_keys.is_empty() {
            let immutable = self.immutable.read();
            for memtable in immutable.iter().rev() {
                let mut i = 0;
                while i < remaining_keys.len() {
                    let (idx, key) = remaining_keys[i];
                    if let Some(entry) = memtable.get(key)? {
                        let mut value = Value {
                            data: entry.data,
                            timestamp: entry.timestamp,
                            deleted: entry.deleted,
                        };
                        if let ValueData::Blob(ref blob_ref) = value.data {
                            if let Ok(blob_data) = self.blob_store.get(blob_ref) {
                                value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
                            }
                        }
                        if !value.deleted {
                            results[idx] = Some(value);
                        }
                        remaining_keys.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                if remaining_keys.is_empty() { break; }
            }
        }

        if remaining_keys.is_empty() {
            debug_log!("[batch_get] all keys found in memory, skip SSTable");
            return Ok(results);
        }
        
        // 3. Check SSTables (对剩余的keys进行查询)
        debug_log!("🔍 [batch_get] 开始查询SSTables，剩余 {} 个keys", remaining_keys.len());
        let sstable_metas = self.compaction_worker.get_all_sstables()?;
        debug_log!("  📂 [batch_get] 共有 {} 个SSTables", sstable_metas.len());
        
        for level in 0..self.config.num_levels {
            let level_sstables: Vec<_> = sstable_metas.iter()
                .filter(|meta| self.get_level_from_path(&meta.path) == level)
                .collect();
            
            if level_sstables.is_empty() {
                continue;
            }
            
            debug_log!("  🔍 [batch_get] 查询Level {} ({} 个SSTables)", level, level_sstables.len());
            
            // 🚀 P3+: 批量查询每个SSTable（使用 batch_get）
            for meta in level_sstables.iter().rev() {
                // 预过滤：只查询在key range内的keys
                let keys_in_range: Vec<(usize, Key)> = remaining_keys.iter()
                    .filter(|(_, key)| *key >= meta.min_key && *key <= meta.max_key)
                    .copied()
                    .collect();
                
                if keys_in_range.is_empty() {
                    continue; // 没有key在这个SSTable的范围内
                }
                
                // Use cached SSTable handle
                let cached = match self.sstable_cache.get_or_open(&meta.path) {
                    Ok(cached) => cached,
                    Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        continue; // 文件已删除，跳过
                    }
                    Err(e) => return Err(e),
                };

                // 🔥 P3+: 批量查询（使用 SSTable::batch_get）
                let sstable = cached.handle.read();
                
                // 提取 keys（只保留 key，不包含 idx）
                let query_keys: Vec<Key> = keys_in_range.iter().map(|(_, key)| *key).collect();
                
                // 🚀 批量查询（利用批量 Bloom Filter 检查）
                let batch_results = sstable.batch_get(&query_keys)?;
                
                // 处理批量查询结果
                for (i, (idx, _key)) in keys_in_range.iter().enumerate() {
                    if let Some(mut value) = batch_results[i].clone() {
                        // Resolve blob reference
                        if let ValueData::Blob(ref blob_ref) = value.data {
                            let blob_data = self.blob_store.get(blob_ref)?;
                            value.data = ValueData::Inline(std::sync::Arc::new(blob_data));
                        }
                        
                        // Don't add tombstones to results (keep as None)
                        if !value.deleted {
                            results[*idx] = Some(value);
                        }
                        
                        // 从 remaining_keys 中移除
                        if let Some(pos) = remaining_keys.iter().position(|(i, _)| *i == *idx) {
                            remaining_keys.swap_remove(pos);
                        }
                    }
                }
                // 🔓 SSTable锁在这里释放（批量处理完成）
                
                if remaining_keys.is_empty() {
                    debug_log!("  ✅ [batch_get] 所有keys已找到，提前退出Level {}", level);
                    break;
                }
            }
            
            if remaining_keys.is_empty() {
                break;
            }
        }
        
        debug_log!("✅ [batch_get] 批量查询完成，返回 {} 个结果，{} 个未找到", 
                 results.iter().filter(|r| r.is_some()).count(), 
                 remaining_keys.len());
        Ok(results)
    }
    
    /// 🚀 P2 优化：真正的批量插入
    /// 
    /// ## 优化要点
    /// - 直接调用 MemTable::batch_put()（单次加锁）
    /// - 批量检查是否需要 rotate
    /// - 减少锁竞争，提升 3-5 倍性能
    /// 
    /// ## 性能对比
    /// - 旧版本：1000 条 = 1000 次 put() = 1000 次加锁
    /// - 新版本：1000 条 = 1 次 batch_put() = 1 次加锁
    pub fn batch_put(&self, kvs: &[(Key, Value)]) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        // Offload large values to blob storage before inserting into memtable
        let processed: Vec<(Key, Value)> = kvs.iter().map(|(k, v)| {
            let mut v = v.clone();
            if let ValueData::Inline(ref data) = v.data {
                if data.len() >= self.config.blob_threshold {
                    if let Ok(blob_ref) = self.blob_store.put(data) {
                        v.data = ValueData::Blob(blob_ref);
                    }
                }
            }
            (k.clone(), v)
        }).collect();

        // Process in chunks to apply backpressure when memtable fills up.
        // Without this, a batch of 1M rows would grow the memtable unboundedly.
        const CHUNK_SIZE: usize = 1024;

        for chunk in processed.chunks(CHUNK_SIZE) {
            // Same backpressure logic as put()
            let mut backpressure_count = 0;
            loop {
                let should_rotate = {
                    let memtable = self.memtable.read();
                    memtable.should_flush()
                };

                if !should_rotate {
                    break;
                }

                let queue_len = {
                    let immutable = self.immutable.read();
                    immutable.len()
                };

                if queue_len < self.max_immutable_slots
                    && self.try_rotate_memtable().is_ok() {
                        break;
                    }

                backpressure_count += 1;
                if backpressure_count > 10000 {
                    return Err(StorageError::Transaction(
                        "LSM backpressure timeout during batch_put".into()
                    ));
                }
                thread::sleep(Duration::from_millis(10));
            }

            let memtable = self.memtable.read();
            memtable.batch_put(chunk)?;
        }

        Ok(())
    }
    
    /// Delete a key
    pub fn delete(&self, key: Key, timestamp: u64) -> Result<()> {
        self.put(key, Value::tombstone(timestamp))
    }

    /// Delete all keys in [start, end] range by inserting individual tombstones.
    ///
    /// For TTL GC and DROP TABLE: scans the memtable for keys in range and
    /// marks each as deleted. On-disk SSTable entries are handled during
    /// compaction when they encounter these tombstones.
    ///
    /// # Performance
    /// - Memtable keys: O(k) where k = keys in range (BTree range scan)
    /// - SSTable keys: tombstones written to memtable, cleaned up by compaction
    pub fn delete_range(&self, start: Key, end: Key, timestamp: u64) -> Result<usize> {
        if start > end {
            return Ok(0);
        }

        let tombstone = Value::tombstone(timestamp);
        let mut count = 0;

        // Scan ALL sources (memtable, immutable, SSTables) for keys in range
        // and write individual tombstones. This is O(N) in keys but correct.
        match self.scan_range(start, end) {
            Ok(entries) => {
                for (key, _value) in &entries {
                    self.put(*key, tombstone.clone())?;
                    count += 1;
                }
            }
            Err(_) => {
                // Fallback: write tombstones at boundaries as best-effort
                self.put(start, tombstone.clone())?;
                self.put(end, tombstone)?;
                count = 2;
            }
        }

        Ok(count)
    }

    /// Resolve a blob reference to its actual data.
    /// Used by index builders to access large values stored in blob files.
    pub fn resolve_blob(&self, blob_ref: &super::BlobRef) -> Result<Vec<u8>> {
        self.blob_store.get(blob_ref)
    }

    /// 🆕 Insert data with vector (for vector-enabled MemTable)
    /// 
    /// ## Parameters
    /// - `key`: row_id
    /// - `data`: row data (protobuf bytes)
    /// - `vector`: embedding vector
    /// - `timestamp`: MVCC timestamp
    /// 
    /// ## Performance
    /// - 插入延迟: ~2μs (内存写 + 图索引)
    /// - 图索引: O(log n) 平均，O(R log n) 最坏
    pub fn put_with_vector(&self, key: Key, mut data: ValueData, vector: Vec<f32>, timestamp: u64) -> Result<()> {
        // Check if value should go to blob storage
        if let ValueData::Inline(ref inline_data) = data {
            if inline_data.len() >= self.config.blob_threshold {
                // Move large value to blob store
                let blob_ref = self.blob_store.put(inline_data)?;
                data = ValueData::Blob(blob_ref);
            }
        }

        // Same backpressure logic as put() to prevent OOM
        let mut backpressure_count = 0;
        loop {
            {
                let memtable = self.memtable.read();

                if !memtable.should_flush() {
                    memtable.put_with_vector(key, data, vector, timestamp)?;
                    return Ok(());
                }
            }

            let queue_len = {
                let immutable = self.immutable.read();
                immutable.len()
            };

            if queue_len < self.max_immutable_slots
                && self.try_rotate_memtable().is_ok() {
                    continue;
                }

            backpressure_count += 1;
            if backpressure_count > 10000 {
                return Err(StorageError::Transaction(
                    "LSM backpressure timeout in put_with_vector".into()
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }

    /// Flush all memtables to disk (THREAD-SAFE: 使用互斥锁防止并发 flush)
    /// 
    /// 🔥 NEW: Flushes entire immutable queue + active memtable
    pub fn flush(&self) -> Result<()> {
        self.flush_with_paths().map(|_| ())
    }
    
    /// 🆕 Flush and return paths of newly created SSTables
    /// 
    /// This allows Database layer to backfill indexes from flushed data.
    pub fn flush_with_paths(&self) -> Result<Vec<PathBuf>> {
        debug_log!("💾 [flush] 开始flush操作...");
        // 🔧 检查存储目录是否存在（防止在数据库关闭后flush）
        if !self.storage_dir.exists() {
            debug_log!("⚠️  [flush] 存储目录不存在，跳过flush: {:?}", self.storage_dir);
            return Ok(Vec::new());
        }

        // 0. Sync blob store — ensures large values are durable BEFORE
        //    any SSTable referencing them is created and synced.
        //    Without this, a crash between SSTable fsync and blob data reaching
        //    disk would cause CRC errors on recovery (recoverable via WAL, but
        //    better to prevent the inconsistency).
        self.blob_store.flush()?;

        // 1. Force rotate active MemTable (even if not full)
        // 🔥 CRITICAL: Use a scope to release flush_lock immediately after rotate
        {
            let _flush_guard = self.flush_lock.lock()
                .map_err(|_| StorageError::Lock("Flush lock poisoned".into()))?;

            let has_data = {
                let memtable = self.memtable.read();
                !memtable.is_empty()
            };

            if has_data {
                self.rotate_memtable()?;
            }
        }

        // 2. Wait for background thread to flush the queue using condvar
        let start_wait = std::time::Instant::now();
        {
            let (lock, cvar) = &*self.flush_wakeup;
            let mut guard = lock.lock()
                .map_err(|_| StorageError::Lock("flush_wakeup lock poisoned".into()))?;

            loop {
                let queue_len = {
                    let immutable = self.immutable.read();
                    immutable.len()
                };

                if queue_len == 0 {
                    break;
                }

                if start_wait.elapsed().as_secs() > 120 {
                    return Err(StorageError::Transaction(
                        "Flush timeout: background thread may be stuck".into(),
                    ));
                }

                let result = cvar.wait_timeout(guard, Duration::from_millis(100));
                match result {
                    Ok((timeout_guard, _timed_out)) => guard = timeout_guard,
                    Err(_) => {
                        return Err(StorageError::Lock("flush_wakeup lock poisoned".into()));
                    }
                }
            }
        }
        
        debug_log!("✅ [flush] 整个flush操作完成");
        Ok(Vec::new())  // 不再返回 sstable_paths，因为是后台线程写入的
    }
    
    /// 🚀 Unified Flush Callback
    /// 
    /// Registers a callback that will be called during flush:
    /// - Input: &UnifiedMemTable (reference to the flushing MemTable)
    /// - Called **before** SSTable is written to disk
    /// - Allows Database layer to batch build all indexes
    /// 
    /// ✅ 统一入口：手动Flush和后台Flush都会触发此回调
    /// ✅ 高效：传入MemTable引用，避免数据拷贝
    /// Force compaction: run one compaction cycle (best-effort).
    /// Returns true if more compaction is needed.
    pub fn compact(&self) -> Result<bool> {
        let needs = self.compaction_worker.needs_compaction()?;
        if needs {
            self.compaction_worker.run_compaction()?;
        }
        Ok(needs)
    }

    pub fn set_flush_callback<F>(&self, callback: F) -> Result<()>
    where
        F: Fn(&UnifiedMemTable) -> Result<()> + Send + Sync + 'static,
    {
        let mut cb = self.flush_callback.write();
        *cb = Some(Arc::new(callback));
        Ok(())
    }
    
    // Internal helpers
    
    fn get_level_from_path(&self, path: &std::path::Path) -> usize {
        // Parse level from filename: "l0_000001.sst" -> 0
        path.file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("l"))
            .and_then(|s| s.split('_').next())
            .and_then(|level_str| level_str.parse::<usize>().ok())
            .unwrap_or(0)
    }
    
    /// Try to rotate MemTable (non-blocking if queue is full)
    /// 
    /// ## Multi-slot Queue Logic
    /// - Check if immutable queue has space (< max_slots)
    /// - If has space: push active → queue, create new active
    /// - If full: skip rotation (caller will apply backpressure)
    fn try_rotate_memtable(&self) -> Result<()> {
        // Quick check: is queue full?
        {
            let immutable = self.immutable.read();
            if immutable.len() >= self.max_immutable_slots {
                return Err(StorageError::Transaction("Immutable queue full".into()));
            }
        }

        // Acquire both locks for atomic swap (lock order: memtable → immutable, same as get/scan)
        {
            let mut memtable_lock = self.memtable.write();
            let mut immutable_lock = self.immutable.write();

            if immutable_lock.len() >= self.max_immutable_slots {
                return Err(StorageError::Transaction("Immutable queue full".into()));
            }

            let new_memtable = Self::create_memtable(&self.config, &memtable_lock);
            let old_memtable = std::mem::replace(&mut *memtable_lock, new_memtable);
            if !old_memtable.is_empty() {
                immutable_lock.push_back(old_memtable);
                self.rotation_epoch.fetch_add(1, Ordering::Release);
            }
        }
        Ok(())
    }

    /// Force rotate (blocking, used by flush())
    fn rotate_memtable(&self) -> Result<()> {
        // Wait until queue has space
        let mut wait_count = 0;
        loop {
            {
                let immutable = self.immutable.read();
                if immutable.len() < self.max_immutable_slots {
                    break;
                }
                if wait_count % 1000 == 0 && wait_count > 0 {
                    debug_log!("[rotate_memtable] ⏳ Waiting {}ms (queue: {}/{})",
                        wait_count, immutable.len(), self.max_immutable_slots);
                }
            }
            // Sleep briefly to avoid busy loop
            thread::sleep(Duration::from_millis(1));
            wait_count += 1;
            
            if wait_count > 120000 {
                return Err(StorageError::Transaction("rotate_memtable timeout: deadlock?".into()));
            }
        }
        
        // Now rotate (lock order: memtable → immutable, consistent with get/scan)
        let mut memtable_lock = self.memtable.write();
        let mut immutable_lock = self.immutable.write();

        let new_memtable = Self::create_memtable(&self.config, &memtable_lock);

        let old_memtable = std::mem::replace(&mut *memtable_lock, new_memtable);
        immutable_lock.push_back(old_memtable);
        self.rotation_epoch.fetch_add(1, Ordering::Release);

        // Notify background flush thread (this is an explicit flush, wake immediately)
        drop(immutable_lock);
        drop(memtable_lock);
        {
            let (lock, cvar) = &*self.flush_wakeup;
            if let Ok(mut guard) = lock.lock() {
                *guard = true;
            }
            cvar.notify_all();
        }

        Ok(())
    }
    
    /// 🆕 Helper: Create a new UnifiedMemTable matching the existing one's configuration
    fn create_memtable(config: &LSMConfig, existing: &UnifiedMemTable) -> UnifiedMemTable {
        // Check if existing memtable has vector support
        if let Some(dimension) = existing.vector_dimension() {
            UnifiedMemTable::new_with_vector_support(config, dimension)
        } else {
            UnifiedMemTable::new(config)
        }
    }
    


    /// Scan MemTable (including immutable) with zero-copy callback
    /// 
    /// ✅ Zero-copy optimization: No Vec allocation, processes items in-place
    pub fn scan_memtable_with<F>(&self, start: Key, end: Key, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &[u8]) -> Result<()>,
    {
        use std::collections::BTreeMap;

        // BTreeMap: keeps keys sorted naturally, avoids separate sort step
        let mut merged: BTreeMap<Key, std::sync::Arc<Vec<u8>>> = BTreeMap::new();

        // 1. Scan immutable queue (oldest to newest, so newer values overwrite)
        {
            let immutable = self.immutable.read();

            for mem in immutable.iter() {
                let entries = mem.scan(start, end)?;
                for (k, entry) in entries {
                    if entry.deleted { continue; }
                    match &entry.data {
                        ValueData::Inline(d) => { merged.insert(k, d.clone()); }
                        ValueData::Blob(blob_ref) => {
                            if let Ok(data) = self.blob_store.get(blob_ref) {
                                merged.insert(k, std::sync::Arc::new(data));
                            }
                        }
                    }
                }
            }
        }

        // 2. Scan active MemTable (overwrites older values)
        {
            let memtable = self.memtable.read();
            let entries = memtable.scan(start, end)?;
            for (k, entry) in entries {
                if entry.deleted { continue; }
                match &entry.data {
                    ValueData::Inline(d) => { merged.insert(k, d.clone()); }
                    ValueData::Blob(blob_ref) => {
                        if let Ok(data) = self.blob_store.get(blob_ref) {
                            merged.insert(k, std::sync::Arc::new(data));
                        }
                    }
                }
            }
        }

        // 3. Process merged results (already sorted by BTreeMap)
        for (key, data) in &merged {
            f(*key, data)?;
        }

        Ok(())
    }
    
    /// Scan MemTable (including immutable) for a key range [start, end) - Legacy API
    /// 
    /// ⚠️ Prefer scan_memtable_with() for zero-copy iteration
    pub fn scan_memtable(&self, start: Key, end: Key) -> Result<Vec<(Key, Vec<u8>)>> {
        // 🚀 P3 优化：预分配容量（估算范围大小）
        let estimated_size = ((end - start) as usize).min(1000);
        let mut results = Vec::with_capacity(estimated_size);
        self.scan_memtable_with(start, end, |k, v| {
            results.push((k, v.to_vec()));
            Ok(())
        })?;
        Ok(results)
    }
    
    /// Scan all MemTable entries with zero-copy callback

    /// 🔧 优化方法：只扫描增量数据 (active + immutable MemTable) - Zero-copy version
    /// 已 flush 到 SSTable 的数据应该走持久化索引 + LRU 缓存
    /// 
    /// ✅ Zero-copy optimization: Uses callback to avoid Vec allocation
    /// ⚠️  CRITICAL: 先收集数据，释放锁后再调用回调，避免在持锁期间执行慢操作导致阻塞
    pub fn scan_memtable_incremental_with<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &[u8]) -> Result<()>,
    {
        // Step 1: 收集所有数据（持锁时间最小化）
        let mut all_entries = Vec::new();
        
        // 1.1 扫描 immutable queue (等待 flush 的数据)
        {
            let immutable = self.immutable.read();
            
            for memtable in immutable.iter() {
                let entries = memtable.scan_all()?;
                for (k, entry) in entries {
                    // 🔧 FIX: Skip tombstones (deleted entries)
                    if entry.deleted {
                        continue;
                    }
                    
                    match &entry.data {
                        ValueData::Inline(d) => {
                            all_entries.push((k, d.clone()));
                        },
                        ValueData::Blob(blob_ref) => {
                            match self.blob_store.get(blob_ref) {
                                Ok(data) => all_entries.push((k, std::sync::Arc::new(data))),
                                Err(_) => {}, // blob resolution failed, skip entry
                            }
                        },
                    }
                }
            }
        }

        // 1.2 Scan active MemTable
        {
            let memtable = self.memtable.read();
            let entries = memtable.scan_all()?;
            for (k, entry) in entries {
                if entry.deleted {
                    continue;
                }

                match &entry.data {
                    ValueData::Inline(d) => {
                        all_entries.push((k, d.clone()));
                    },
                    ValueData::Blob(blob_ref) => {
                        match self.blob_store.get(blob_ref) {
                            Ok(data) => all_entries.push((k, std::sync::Arc::new(data))),
                            Err(_) => {}, // blob resolution failed, skip entry
                        }
                    },
                }
            }
        }
        
        // Step 2: 释放所有锁后，再调用回调处理数据（避免在持锁期间执行慢操作）
        for (k, data) in all_entries {
            f(k, &data)?;
        }
        
        Ok(())
    }
    
    /// 🔧 优化方法：只扫描增量数据 (active + immutable MemTable) - Legacy API
    /// 
    /// ⚠️ Prefer scan_memtable_incremental_with() for zero-copy iteration
    pub fn scan_memtable_incremental(&self) -> Result<Vec<(Key, Vec<u8>)>> {
        // 🚀 P3 优化：预分配容量
        let mut results = Vec::with_capacity(100);
        self.scan_memtable_incremental_with(|k, v| {
            results.push((k, v.to_vec()));
            Ok(())
        })?;
        Ok(results)
    }

    /// 🆕 Public API: Force rotate active MemTable to immutable queue
    /// 
    /// Blocks until immutable queue has space (backpressure control)
    pub fn force_rotate(&self) -> Result<()> {
        self.rotate_memtable()
    }
    
    /// 🆕 Public API: Get immutable queue size
    pub fn immutable_queue_len(&self) -> usize {
        self.immutable.read().len()
    }
    
    /// 🚀 Complete range scan: MemTable + Immutable + SSTables
    /// 
    /// This is the CORRECT way to scan a key range in LSM-Tree.
    /// Returns all non-deleted entries in [start, end), deduplicated by latest version.
    /// 
    /// # Performance
    /// - MemTable scan: O(log N + K) where K = result size
    /// - SSTable scan: O(B × log M) where B = number of blocks, M = entries per block
    /// - Merge: O(K log K) where K = total results
    /// 
    /// # Example
    /// ```ignore
    /// let rows = engine.scan_range(start_key, end_key)?;
    /// ```ignore
    pub fn scan_range(&self, start: Key, end: Key) -> Result<Vec<(Key, Value)>> {
        // Use streaming merge instead of materializing into BTreeMap.
        // Memory: O(sources) instead of O(total results).
        let iter = self.scan_range_streaming(start, end)?;
        iter.collect()
    }
    

    /// Get level statistics
    pub fn level_stats(&self) -> Result<Vec<(usize, usize, u64)>> {
        self.compaction_worker.level_stats()
    }
    
    /// Estimate key count in a given range (fast, O(1))
    /// 
    /// Uses SSTable metadata to estimate count without reading actual data.
    /// Useful for query optimization (index selectivity calculation).
    /// 
    /// # Performance
    /// - Full scan: O(n) - 300ms for 300K keys
    /// - Estimation: O(1) - <1ms (reads metadata only)
    /// 
    /// # Accuracy
    /// - ±10% error rate (due to overlapping SSTables and tombstones)
    /// - Accurate enough for query planning
    /// 
    /// # Example
    /// ```ignore
    /// let count = engine.estimate_key_count_in_range(start, end)?;
    /// // count ≈ 100,000 (actual: 90,000-110,000)
    /// ```
    pub fn estimate_key_count_in_range(&self, start: Key, end: Key) -> Result<usize> {
        // Get all SSTable metadata
        let sstable_metas = self.compaction_worker.get_all_sstables()?;
        
        let mut estimated_count = 0usize;
        
        for meta in sstable_metas.iter() {
            // Check if SSTable key range overlaps with [start, end)
            if meta.min_key < end && meta.max_key >= start {
                // Overlap detected, add entry count
                // Note: This may overcount due to overlapping SSTables
                estimated_count += meta.num_entries as usize;
            }
        }
        
        Ok(estimated_count)
    }
    
    /// 🚀 流式范围扫描（批量迭代器，内存友好）
    /// 
    /// 返回一个迭代器，每次产出一批数据（默认 1000 条），而不是一次性加载全部。
    ///
    /// # 性能对比
    /// - `scan_range()`: 30 万条 × 1.4 KB = 420 MB 内存峰值 🔴
    /// - `scan_range_batched()`: 仍需构建完整 BTreeMap 后分批返回，峰值内存同 scan_range() 🔴
    /// - `scan_range_streaming()`: 13 个迭代器 × 1.5 KB = 20 KB ✅ (推荐)
    ///
    /// # 示例
    /// ```ignore
    /// for batch_result in engine.scan_range_batched(start, end, 1000)? {
    ///     let batch = batch_result?;
    ///     for (key, value) in batch {
    ///         // 处理每条数据
    ///     }
    /// }
    /// ```
    pub fn scan_range_batched(&self, start: Key, end: Key, batch_size: usize) -> Result<LSMBatchedIterator> {
        // Use streaming iterator internally to avoid BTreeMap materialization
        let iter = self.scan_range_streaming(start, end)?;
        let mut all_data = Vec::new();
        for item in iter {
            all_data.push(item?);
        }
        Ok(LSMBatchedIterator {
            data: all_data,
            batch_size,
            current_pos: 0,
        })
    }
}

/// 🚀 批量迭代器：每次返回一批数据
pub struct LSMBatchedIterator {
    data: Vec<(Key, Value)>,
    batch_size: usize,
    current_pos: usize,
}

impl Iterator for LSMBatchedIterator {
    type Item = Result<Vec<(Key, Value)>>;
    
    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.data.len() {
            return None;
        }
        
        let end_pos = (self.current_pos + self.batch_size).min(self.data.len());
        let batch = self.data[self.current_pos..end_pos].to_vec();
        self.current_pos = end_pos;
        
        Some(Ok(batch))
    }
}

// 继续 LSMEngine 的实现
impl LSMEngine {
    /// 🚀 真正的流式范围扫描（O(1) 内存占用）
    /// 
    /// 使用多路归并迭代器，逐个返回 key-value，不预先合并所有数据到内存。
    /// 
    /// # 内存对比
    /// - `scan_range()`: 30万条 × 1.4 KB = 420 MB 🔴
    /// - `scan_range_streaming()`: 13 个迭代器 × 1.5 KB = 20 KB ✅
    /// - **节省 99.995% 内存**
    /// 
    /// # 示例
    /// ```ignore
    /// for result in engine.scan_range_streaming(start, end)? {
    ///     let (key, value) = result?;
    ///     // 🚀 每次只在内存中保留一条记录！
    /// }
    /// ```
    pub fn scan_range_streaming(&self, start: Key, end: Key) -> Result<super::MergingIterator> {
        let mut sources: Vec<KVIterator> = Vec::new();

        // Loop until we get a consistent snapshot (epoch stable across the entire snapshot).
        // This prevents data loss when auto-flush rotates MemTable → Immutable → SSTable
        // or when compaction replaces SSTables concurrently with our scan.
        const MAX_RETRIES: u32 = 10;
        let mut retries = 0;
        loop {
            sources.clear();

            let rot_epoch_before = self.rotation_epoch.load(Ordering::Acquire);
            let cmp_epoch_before = self.compaction_worker.compaction_epoch().load(Ordering::Acquire);

            // Phase 1: Snapshot memtable + immutable (under read locks)
            {
                let memtable = self.memtable.read();
                let immutable = self.immutable.read();

                // Source 1: Active MemTable — zero-copy Arc<DataEntry>
                {
                    let entries = memtable.scan_arcs(start, end);
                    if !entries.is_empty() {
                        let iter = entries.into_iter().map(|(k, arc)| {
                            Ok((k, Value {
                                data: arc.data.clone(),
                                timestamp: arc.timestamp,
                                deleted: arc.deleted,
                            }))
                        });
                        sources.push(Box::new(iter));
                    }
                }

                // Source 2-N: Immutable queue — zero-copy Arc<DataEntry>
                for mt in immutable.iter() {
                    let entries = mt.scan_arcs(start, end);
                    if !entries.is_empty() {
                        let iter = entries.into_iter().map(|(k, arc)| {
                            Ok((k, Value {
                                data: arc.data.clone(),
                                timestamp: arc.timestamp,
                                deleted: arc.deleted,
                            }))
                        });
                        sources.push(Box::new(iter));
                    }
                }
            }

            // Phase 2: SSTables
            let sstable_metas = self.compaction_worker.get_all_sstables()?;

            for meta in sstable_metas.iter().rev() {
                if start > meta.max_key || end <= meta.min_key {
                    continue;
                }

                let cached = match self.sstable_cache.get_or_open(&meta.path) {
                    Ok(cached) => cached,
                    Err(e) => {
                        debug_log!("[scan_range_streaming] Failed to open SSTable {:?}: {:?}", meta.path, e);
                        continue;
                    }
                };

                // 🚀 Streaming SSTable scan — reads blocks on demand, O(1) memory
                let sst_iter = {
                    let sstable = cached.handle.read();
                    match crate::storage::lsm::sstable::SSTableIterator::with_range(
                        &sstable, Some(start), Some(end),
                    ) {
                        Ok(iter) => iter,
                        Err(e) => {
                            debug_log!("[scan_range_streaming] Failed to create SSTable iterator {:?}: {:?}", meta.path, e);
                            continue;
                        }
                    }
                };
                sources.push(Box::new(sst_iter.map(|(k, v)| Ok((k, v)))));
            }

            // Phase 3: Validate consistency — if either epoch changed during our
            // snapshot, data may have moved or SSTables replaced. Retry.
            let rot_epoch_after = self.rotation_epoch.load(Ordering::Acquire);
            let cmp_epoch_after = self.compaction_worker.compaction_epoch().load(Ordering::Acquire);
            if rot_epoch_after == rot_epoch_before && cmp_epoch_after == cmp_epoch_before {
                break; // Consistent snapshot
            }
            retries += 1;
            if retries >= MAX_RETRIES {
                return Err(crate::error::StorageError::ResourceExhausted(format!(
                    "scan_range_streaming: failed to get consistent snapshot after {} retries (rotation_epoch: {}→{}, compaction_epoch: {}→{})",
                    MAX_RETRIES, rot_epoch_before, rot_epoch_after, cmp_epoch_before, cmp_epoch_after
                )));
            }
        }

        Ok(super::MergingIterator::new(sources))
    }
}

impl Drop for LSMEngine {
    fn drop(&mut self) {
        debug_log!("[LSMEngine::Drop] 🛑 Shutting down LSM engine...");

        // 🔧 Step 1: Flush ALL data while background thread is still alive.
        // This rotates active memtable → immutable queue, then waits for the
        // background flush thread to drain the queue via condvar.
        debug_log!("[LSMEngine::Drop] 💾 Flushing data (thread still alive)...");
        if let Err(e) = self.flush() {
            debug_log!("[LSMEngine::Drop] ⚠️  Flush failed: {:?}", e);
        } else {
            debug_log!("[LSMEngine::Drop] ✓ Flush complete");
        }

        // 🔧 Step 2: Signal shutdown and stop background threads
        self.shutdown.store(true, Ordering::Relaxed);
        {
            let (lock, cvar) = &*self.flush_wakeup;
            if let Ok(mut guard) = lock.lock() { *guard = true; }
            cvar.notify_all();
        }
        {
            let (lock, cvar) = &*self.compaction_wakeup;
            if let Ok(mut guard) = lock.lock() { *guard = true; }
            cvar.notify_all();
        }

        if let Some(compaction_thread) = self.compaction_thread.take() {
            debug_log!("[LSMEngine::Drop] ⏳ Waiting for compaction thread...");
            let _ = compaction_thread.join();
            debug_log!("[LSMEngine::Drop] ✓ Compaction thread stopped");
        }

        if let Some(flush_thread) = self.flush_thread.take() {
            debug_log!("[LSMEngine::Drop] ⏳ Waiting for flush thread...");
            let _ = flush_thread.join();
            debug_log!("[LSMEngine::Drop] ✓ Flush thread stopped");
        }

        // 🔧 Step 3: Clear SSTable cache to release file handles
        self.sstable_cache.clear();
        debug_log!("[LSMEngine::Drop] ✓ Cache cleared");

        // Step 4: Flush any deferred SST deletions from last compaction
        self.compaction_worker.flush_pending_deletions();
        debug_log!("[LSMEngine::Drop] ✓ Pending deletions flushed");

        debug_log!("[LSMEngine::Drop] ✅ LSM engine shutdown complete");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    
    #[test]
    fn test_basic_operations() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), LSMConfig::default()).unwrap();
        
        // Put
        engine.put(1u64, Value::new(b"value1".to_vec(), 1)).unwrap();
        engine.put(2u64, Value::new(b"value2".to_vec(), 2)).unwrap();
        
        // Get
        let value = engine.get(1u64).unwrap().unwrap();
        assert_eq!(value.data, ValueData::Inline(std::sync::Arc::new(b"value1".to_vec())));
        
        // Delete
        engine.delete(1u64, 3).unwrap();
        let value = engine.get(1u64).unwrap();
        assert!(value.is_none(), "Deleted key should return None");
    }
    
    #[test]
    fn test_memtable_flush() {
        let temp_dir = TempDir::new().unwrap();
        let config = LSMConfig { memtable_size: 100, ..Default::default() };

        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), config).unwrap();
        
        // Insert enough data to trigger flush
        for i in 0..20u64 {
            let value = Value::new(vec![0u8; 10], i);
            engine.put(i, value).unwrap();
        }
        
        // Explicitly flush
        engine.flush().unwrap();
        
        // Verify SSTable was created
        let sstables: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .collect();
        
        assert!(!sstables.is_empty(), "Should have created at least one SSTable");
    }

    #[test]
    fn test_put_get_1000() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), LSMConfig::default()).unwrap();

        for i in 0..1000u64 {
            engine.put(i, Value::new(i.to_le_bytes().to_vec(), i)).unwrap();
        }
        for i in 0..1000u64 {
            let val = engine.get(i).unwrap().expect("key should exist");
            assert_eq!(val.data, ValueData::Inline(std::sync::Arc::new(i.to_le_bytes().to_vec())));
        }
    }

    #[test]
    fn test_scan_range() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), LSMConfig::default()).unwrap();

        for i in 100..200u64 {
            engine.put(i, Value::new(vec![(i % 256) as u8], i)).unwrap();
        }
        // Insert some keys outside range
        engine.put(50, Value::new(vec![1], 50)).unwrap();
        engine.put(250, Value::new(vec![1], 250)).unwrap();

        let iter = engine.scan_range_streaming(100, 200).unwrap();
        let results: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        assert_eq!(results.len(), 100, "should have exactly 100 entries in range [100,200)");
        for (i, (key, _)) in results.iter().enumerate() {
            assert_eq!(*key, 100 + i as u64);
        }
    }

    #[test]
    fn test_scan_range_after_flush() {
        let temp_dir = TempDir::new().unwrap();
        let config = LSMConfig { memtable_size: 256, ..Default::default() };
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), config).unwrap();

        // Insert enough to trigger flush
        for i in 0..100u64 {
            engine.put(i, Value::new(vec![0u8; 20], i)).unwrap();
        }
        engine.flush().unwrap();

        // Scan after flush — should still see all data
        let iter = engine.scan_range_streaming(0, 100).unwrap();
        let results: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        assert_eq!(results.len(), 100, "all keys should be visible after flush");
    }

    #[test]
    fn test_delete_visibility() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), LSMConfig::default()).unwrap();

        engine.put(1, Value::new(b"alive".to_vec(), 100)).unwrap();
        engine.delete(1, 200).unwrap();

        // Delete with higher timestamp makes key invisible
        assert!(engine.get(1).unwrap().is_none(), "deleted key should be invisible");

        // Put with even higher timestamp revives
        engine.put(1, Value::new(b"revived".to_vec(), 300)).unwrap();
        let val = engine.get(1).unwrap().unwrap();
        assert_eq!(val.data, ValueData::Inline(std::sync::Arc::new(b"revived".to_vec())));
    }

    #[test]
    fn test_scan_streaming_empty_range() {
        let temp_dir = TempDir::new().unwrap();
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), LSMConfig::default()).unwrap();

        let iter = engine.scan_range_streaming(1000, 2000).unwrap();
        let results: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        assert!(results.is_empty());
    }

    // ━━━ Compaction ━━━

    #[test]
    fn test_flush_and_compact_preserves_data() {
        let temp_dir = TempDir::new().unwrap();
        let config = LSMConfig { memtable_size: 512, ..Default::default() };
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), config).unwrap();
        for i in 0..200u64 {
            engine.put(i, Value::new(i.to_le_bytes().to_vec(), i)).unwrap();
        }
        engine.flush().unwrap();
        let _ = engine.compact();
        for i in 0..200u64 {
            assert!(engine.get(i).unwrap().is_some(), "key {} lost after compaction", i);
        }
    }

    #[test]
    fn test_scan_after_compact() {
        let temp_dir = TempDir::new().unwrap();
        let config = LSMConfig { memtable_size: 1024, ..Default::default() };
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), config).unwrap();
        for i in 0..100u64 {
            engine.put(i, Value::new(vec![(i % 256) as u8], i)).unwrap();
        }
        engine.flush().unwrap();
        // Compaction may or may not be needed depending on data size
        let _ = engine.compact();
        let iter = engine.scan_range_streaming(0, 100).unwrap();
        let results: Vec<_> = iter.filter_map(|r| r.ok()).collect();
        assert_eq!(results.len(), 100, "all keys should be visible after flush+compact");
    }

    #[test]
    fn test_data_survives_multiple_flushes() {
        let temp_dir = TempDir::new().unwrap();
        let config = LSMConfig { memtable_size: 1024, ..Default::default() };
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), config).unwrap();
        // Multiple flush rounds
        for round in 0..3 {
            let base = round * 100;
            for i in 0..100u64 {
                engine.put(base + i, Value::new((base + i).to_le_bytes().to_vec(), base + i)).unwrap();
            }
            engine.flush().unwrap();
        }
        for i in 0..300u64 {
            assert!(engine.get(i).unwrap().is_some(), "key {} lost after multi-flush", i);
        }
    }
}
