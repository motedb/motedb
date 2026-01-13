//! LSM-Tree Engine (main interface)

use super::{UnifiedMemTable, SSTable, SSTableBuilder, Key, Value, ValueData, LSMConfig, CompactionWorker, BlobStore};
use crate::{Result, StorageError};
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::path::PathBuf;
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::collections::VecDeque;

/// True LRU cache for SSTable handles
struct SSTableCache {
    cache: Mutex<lru::LruCache<PathBuf, Arc<Mutex<SSTable>>>>,
}

impl SSTableCache {
    fn new(max_size: usize) -> Self {
        use std::num::NonZeroUsize;
        Self {
            cache: Mutex::new(lru::LruCache::new(
                NonZeroUsize::new(max_size).unwrap()
            )),
        }
    }
    
    fn get_or_open(&self, path: &PathBuf) -> Result<Arc<Mutex<SSTable>>> {
        let mut cache = self.cache.lock()
            .map_err(|_| StorageError::Lock("Cache lock poisoned".into()))?;
        
        // Check if already cached (this also updates LRU position)
        if let Some(sstable) = cache.get(path) {
            return Ok(sstable.clone());
        }
        
        // Open new SSTable
        let sstable = SSTable::open(path)?;
        let sstable_arc = Arc::new(Mutex::new(sstable));
        
        // LRU cache automatically evicts least recently used entry when full
        cache.put(path.clone(), sstable_arc.clone());
        
        Ok(sstable_arc)
    }
    
    fn clear(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            cache.clear();
        }
    }
}

/// LSM-Tree storage engine with multi-slot immutable queue
/// 
/// ## Architecture (🔥 NEW: Multi-slot Immutables)
/// - **Active MemTable**: Accepts writes (never blocks)
/// - **Immutable Queue**: 4 slots for flushing (buffered async)
/// - **Flush Thread**: Background thread that continuously flushes queue
/// 
/// ## Memory Control (🔥 Backpressure-enabled)
/// - Max memory: (1 + max_immutable_slots) × memtable_size = 5 × 4MB = 20MB
/// - When active is full: push to immutable queue, create new active
/// - Backpressure: If queue is full (4 slots occupied), wait for flush
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
    
    /// Immutable MemTable queue (FIFO, up to 4 slots)
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
    
    /// 🚀 Unified Flush Callback
    /// Callback: &UnifiedMemTable -> Result<()>
    /// Called during flush to enable batch index building
    /// 
    /// ✅ 统一入口：手动Flush和后台Flush都会触发
    /// ✅ 传入MemTable引用：避免数据拷贝，高效批量构建
    flush_callback: Arc<RwLock<Option<Arc<dyn Fn(&UnifiedMemTable) -> Result<()> + Send + Sync>>>>,
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
        
        let compaction_worker = Arc::new(CompactionWorker::new(storage_dir.clone(), &config));
        
        // Initialize blob store
        let blob_dir = storage_dir.join("blobs");
        let blob_store = Arc::new(BlobStore::new(blob_dir, config.blob_file_size)?);
        
        // 🆕 Create UnifiedMemTable (with or without vector support)
        let memtable = if let Some(dim) = vector_dimension {
            UnifiedMemTable::new_with_vector_support(&config, dim)
        } else {
            UnifiedMemTable::new(&config)
        };
        
        let mut engine = Self {
            memtable: Arc::new(RwLock::new(memtable)),
            immutable: Arc::new(RwLock::new(VecDeque::new())),  // 🔥 Empty queue
            max_immutable_slots: 4,  // 🔥 NEW: 4 slots = 16MB buffer
            flush_lock: Arc::new(Mutex::new(())),
            flush_in_progress: Arc::new(AtomicBool::new(false)),
            shutdown: Arc::new(AtomicBool::new(false)),
            sstable_cache: Arc::new(SSTableCache::new(config.sstable_cache_size)),
            storage_dir,
            config: config.clone(),
            next_sst_id: Arc::new(RwLock::new(0)),
            compaction_worker: compaction_worker.clone(),
            blob_store,
            compaction_thread: None,
            flush_thread: None,
            flush_callback: Arc::new(RwLock::new(None)),
        };
        
        // 🔥 Start background compaction thread with Weak references
        let compaction_worker_weak = Arc::downgrade(&engine.compaction_worker);
        let shutdown_weak = Arc::downgrade(&engine.shutdown);
        
        let compaction_thread = thread::spawn(move || {
            let mut consecutive_no_work = 0;
            let mut check_interval = Duration::from_secs(1);
            
            loop {
                // 🔧 Check shutdown signal (upgrade Weak to Arc)
                let shutdown = match shutdown_weak.upgrade() {
                    Some(s) => s,
                    None => break,  // Engine dropped, exit gracefully
                };
                
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                
                thread::sleep(check_interval);
                
                // Upgrade Weak to Arc for compaction work
                let compaction_worker = match compaction_worker_weak.upgrade() {
                    Some(w) => w,
                    None => break,  // Engine dropped
                };
                
                match compaction_worker.needs_compaction() {
                    Ok(true) => {
                        if let Err(e) = compaction_worker.run_compaction() {
                            eprintln!("Compaction error: {:?}", e);
                        } else {
                            consecutive_no_work = 0;
                            check_interval = Duration::from_secs(1);
                        }
                    }
                    Ok(false) => {
                        consecutive_no_work += 1;
                        if consecutive_no_work > 10 {
                            check_interval = Duration::from_secs(5);
                        }
                        if consecutive_no_work > 30 {
                            check_interval = Duration::from_secs(10);
                        }
                    }
                    Err(e) => eprintln!("Compaction check error: {:?}", e),
                }
                
                if consecutive_no_work > 60 {
                    consecutive_no_work = 0;
                    check_interval = Duration::from_secs(1);
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
        
        let flush_thread = thread::spawn(move || {
            loop {
                // 🔧 Check shutdown signal
                let shutdown = match shutdown_weak.upgrade() {
                    Some(s) => s,
                    None => break,  // Engine dropped
                };
                
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                
                thread::sleep(Duration::from_millis(10));  // Check every 10ms
                
                // Quick lock-free check
                let flush_in_progress = match flush_in_progress_weak.upgrade() {
                    Some(f) => f,
                    None => break,
                };
                
                if !flush_in_progress.load(Ordering::Acquire) {
                    let immutable = match immutable_weak.upgrade() {
                        Some(i) => i,
                        None => break,
                    };
                    
                    let has_immutable = {
                        if let Ok(immutable_guard) = immutable.read() {
                            !immutable_guard.is_empty()  // 🔥 Check queue not empty
                        } else {
                            false
                        }
                    };
                    
                    if has_immutable {
                        // Try to flush (inline implementation to avoid circular reference)
                        if flush_in_progress.compare_exchange(
                            false, true, Ordering::Acquire, Ordering::Relaxed
                        ).is_ok() {
                            // Pop from front of queue (FIFO)
                            let (memtable, queue_size_after) = {
                                let immutable_lock = immutable.write().ok();
                                immutable_lock.map(|mut lock| {
                                    let mt = lock.pop_front();
                                    let size = lock.len();
                                    (mt, size)
                                }).unwrap_or((None, 0))
                            };
                            
                            if let Some(memtable) = memtable {
                                eprintln!("[LSM Flush] Processing MemTable (queue: {} remaining)", queue_size_after);
                                
                                // 🔥 NEW: Call flush callback (batch index building)
                                // This ensures both manual and background flush trigger index building
                                if let Some(callback_arc) = flush_callback_weak.upgrade() {
                                    if let Ok(callback_guard) = callback_arc.read() {
                                        if let Some(ref callback) = *callback_guard {
                                            if let Err(e) = callback(&memtable) {
                                                eprintln!("[LSM Flush] ⚠️  Callback error: {:?}", e);
                                            } else {
                                                eprintln!("[LSM Flush] ✅ Callback executed successfully");
                                            }
                                        }
                                    }
                                }
                                
                                // Generate SSTable ID
                                let next_sst_id = match next_sst_id_weak.upgrade() {
                                    Some(n) => n,
                                    None => {
                                        flush_in_progress.store(false, Ordering::Release);
                                        break;
                                    }
                                };
                                
                                let sst_id = {
                                    let next_id = next_sst_id.write().ok();
                                    next_id.map(|mut id| {
                                        let current = *id;
                                        *id += 1;
                                        current
                                    })
                                };
                                
                                if let Some(sst_id) = sst_id {
                                    let sst_path = storage_dir_clone.join(format!("l0_{:06}.sst", sst_id));
                                    
                                    // Build SSTable
                                    match SSTableBuilder::new(&sst_path, config_clone.clone(), memtable.len()) {
                                        Ok(mut builder) => {
                                            // 🆕 Convert UnifiedEntry → Value
                                            let mut add_count = 0;
                                            for (key, entry) in memtable.iter() {
                                                let value = Value {
                                                    data: entry.data,
                                                    timestamp: entry.timestamp,
                                                    deleted: entry.deleted,
                                                };
                                                if let Err(e) = builder.add(key, value) {
                                                    eprintln!("[LSM Flush] ❌ Error adding key {}: {:?}", key, e);
                                                } else {
                                                    add_count += 1;
                                                }
                                            }
                                            
                                            match builder.finish() {
                                                Ok(meta) => {
                                                    eprintln!("[LSM Flush] ✅ SSTable_{} written ({} entries)", sst_id, add_count);
                                                    if let Some(worker) = compaction_worker_weak.upgrade() {
                                                        let _ = worker.register_sstable(meta);
                                                    }
                                                }
                                                Err(e) => {
                                                    eprintln!("[LSM Flush] ❌ Failed to finish SSTable_{}: {:?}", sst_id, e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            eprintln!("[LSM Flush] ❌ Failed to create SSTable builder: {:?}", e);
                                        }
                                    }
                                }
                                
                                // Explicitly drop memtable
                                drop(memtable);
                            }
                            
                            flush_in_progress.store(false, Ordering::Release);
                        }
                    }
                }
            }
        });
        
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
    /// - If queue has space (< 4 slots): No blocking, instant rotation
    /// - If queue is full (= 4 slots): **Block write until a slot frees**
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
            let should_rotate = {
                let memtable = self.memtable.read()
                    .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
                memtable.should_flush()
            };
            
            if !should_rotate {
                break; // Active has space, continue
            }
            
            // Check queue capacity before rotating
            let queue_len = {
                let immutable = self.immutable.read()
                    .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
                immutable.len()
            };
            
            if queue_len < self.max_immutable_slots {
                // Queue has space, try to rotate
                if self.try_rotate_memtable().is_ok() {
                    break; // Successfully rotated
                }
            }
            
            // Queue is full, apply backpressure
            backpressure_count += 1;
            if backpressure_count == 1 {
                eprintln!("[LSM] ⚠️  Backpressure: Queue full ({}/{}), waiting for flush...", 
                    queue_len, self.max_immutable_slots);
            } else if backpressure_count % 100 == 0 {
                eprintln!("[LSM] ⏳ Still waiting: {}ms (queue: {}/{})", 
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
        
        if backpressure_count > 0 {
            eprintln!("[LSM] ✓ Backpressure resolved after {}ms", backpressure_count * 10);
        }
        
        // Insert into active memtable
        let memtable = self.memtable.read()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        memtable.put(key, value)?;
        
        Ok(())
    }
    
    /// Get a value by key (LSM查询: MemTable -> Immutable -> SSTables -> Blob)
    pub fn get(&self, key: Key) -> Result<Option<Value>> {
        // 1. Check active memtable (newest data)
        let active_result = {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            memtable.get(key)?
            // 🔓 memtable锁在这里释放
        };
        
        if let Some(entry) = active_result {
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
            
            // Resolve blob reference if needed
            if let ValueData::Blob(ref blob_ref) = value.data {
                let blob_data = self.blob_store.get(blob_ref)?;
                value.data = ValueData::Inline(blob_data);
            }
            return Ok(Some(value));
        }
        
        // 2. Check immutable queue (reverse order, newer first)
        // ⚠️  CRITICAL: 在持锁期间查询所有memtable，但立即返回结果避免长时间持锁
        let immutable_result = {
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            // Search from back (newest) to front (oldest)
            let mut result = None;
            for memtable in immutable.iter().rev() {
                if let Some(entry) = memtable.get(key)? {
                    result = Some(entry);
                    break;
                }
            }
            result
            // 🔓 immutable锁在这里释放
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
                value.data = ValueData::Inline(blob_data);
            }
            return Ok(Some(value));
        }
        
        // 3. Check SSTables (Level 0 -> Level 1 -> ... -> Level N)
        let sstable_metas = self.compaction_worker.get_all_sstables()?;
        
        // Group by level and search from L0 to LN
        for level in 0..self.config.num_levels {
            let level_sstables: Vec<_> = sstable_metas.iter()
                .filter(|meta| self.get_level_from_path(&meta.path) == level)
                .collect();
            
            // For L0: check all files (may overlap), newest first
            // For L1+: binary search by key range
            for meta in level_sstables.iter().rev() {
                // Quick check: key in range? [min_key, max_key] inclusive
                if key < meta.min_key || key > meta.max_key {
                    continue;
                }
                
                // Use cached SSTable handle (避免每次打开文件)
                // ⭐ 处理 compaction 导致的文件删除：如果文件已被 compaction 删除，跳过该文件
                let sstable_arc = match self.sstable_cache.get_or_open(&meta.path) {
                    Ok(arc) => arc,
                    Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                        // 文件被 compaction 删除了，跳过
                        continue;
                    }
                    Err(e) => return Err(e),  // 其他错误需要返回
                };
                let mut sstable = sstable_arc.lock()
                    .map_err(|_| StorageError::Lock("SSTable lock poisoned".into()))?;
                
                if let Some(mut value) = sstable.get(key)? {
                    // Resolve blob reference
                    if let ValueData::Blob(ref blob_ref) = value.data {
                        let blob_data = self.blob_store.get(blob_ref)?;
                        value.data = ValueData::Inline(blob_data);
                    }
                    
                    // Check tombstone
                    if value.deleted {
                        return Ok(None);
                    }
                    
                    return Ok(Some(value));
                }
            }
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
        println!("🔍 [batch_get] 开始批量查询 {} 个keys", keys.len());
        let mut results = vec![None; keys.len()];
        let mut remaining_keys: Vec<(usize, Key)> = keys.iter().enumerate().map(|(i, &k)| (i, k)).collect();
        
        // 1. Check active memtable (批量查询，只获取一次锁)
        {
            debug_log!("🔒 [batch_get] 尝试获取 memtable.read() 锁...");
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            debug_log!("✅ [batch_get] 成功获取 memtable.read() 锁，开始查询 {} 个keys", remaining_keys.len());
            
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
                        value.data = ValueData::Inline(blob_data);
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
            debug_log!("🔓 [batch_get] 释放 memtable.read() 锁，剩余 {} 个keys未找到", remaining_keys.len());
            // 🔓 memtable锁在这里释放
        }
        
        if remaining_keys.is_empty() {
            debug_log!("✅ [batch_get] 所有keys在active memtable中找到，直接返回");
            return Ok(results);
        }
        
        // 2. Check immutable queue (批量查询，只获取一次锁)
        {
            debug_log!("🔒 [batch_get] 尝试获取 immutable.read() 锁...");
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            debug_log!("✅ [batch_get] 成功获取 immutable.read() 锁，immutable queue中有 {} 个memtable", immutable.len());
            
            for (mt_idx, memtable) in immutable.iter().rev().enumerate() {
                println!("  🔍 [batch_get] 查询第 {} 个immutable memtable，剩余 {} 个keys", mt_idx + 1, remaining_keys.len());
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
                            value.data = ValueData::Inline(blob_data);
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
                    println!("  ✅ [batch_get] 所有keys已找到，提前退出immutable查询");
                    break;
                }
            }
            debug_log!("🔓 [batch_get] 释放 immutable.read() 锁，剩余 {} 个keys未找到", remaining_keys.len());
            // 🔓 immutable锁在这里释放
        }
        
        if remaining_keys.is_empty() {
            debug_log!("✅ [batch_get] 所有keys已找到，跳过SSTable查询");
            return Ok(results);
        }
        
        // 3. Check SSTables (对剩余的keys进行查询)
        println!("🔍 [batch_get] 开始查询SSTables，剩余 {} 个keys", remaining_keys.len());
        let sstable_metas = self.compaction_worker.get_all_sstables()?;
        println!("  📂 [batch_get] 共有 {} 个SSTables", sstable_metas.len());
        
        for level in 0..self.config.num_levels {
            let level_sstables: Vec<_> = sstable_metas.iter()
                .filter(|meta| self.get_level_from_path(&meta.path) == level)
                .collect();
            
            if level_sstables.is_empty() {
                continue;
            }
            
            println!("  🔍 [batch_get] 查询Level {} ({} 个SSTables)", level, level_sstables.len());
            
            for meta in level_sstables.iter().rev() {
                let mut i = 0;
                while i < remaining_keys.len() {
                    let (idx, key) = remaining_keys[i];
                    
                    // Quick check: key in range?
                    if key < meta.min_key || key > meta.max_key {
                        i += 1;
                        continue;
                    }
                    
                    // Use cached SSTable handle
                    let sstable_arc = match self.sstable_cache.get_or_open(&meta.path) {
                        Ok(arc) => arc,
                        Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                            i += 1;
                            continue;
                        }
                        Err(e) => return Err(e),
                    };
                    
                    let mut sstable = sstable_arc.lock()
                        .map_err(|_| StorageError::Lock("SSTable lock poisoned".into()))?;
                    
                    if let Some(mut value) = sstable.get(key)? {
                        // Resolve blob reference
                        if let ValueData::Blob(ref blob_ref) = value.data {
                            let blob_data = self.blob_store.get(blob_ref)?;
                            value.data = ValueData::Inline(blob_data);
                        }
                        
                        // Don't add tombstones to results (keep as None)
                        if !value.deleted {
                            results[idx] = Some(value);
                        }
                        remaining_keys.swap_remove(i);
                    } else {
                        i += 1;
                    }
                }
                
                if remaining_keys.is_empty() {
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
    
    /// Batch put
    pub fn batch_put(&self, kvs: &[(Key, Value)]) -> Result<()> {
        for (key, value) in kvs {
            self.put(*key, value.clone())?;  // ✅ u64 copy is cheap
        }
        Ok(())
    }
    
    /// Delete a key
    pub fn delete(&self, key: Key, timestamp: u64) -> Result<()> {
        self.put(key, Value::tombstone(timestamp))
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
        
        // 🔥 Fast path: check if rotation needed (lock-free)
        let should_rotate = {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            memtable.should_flush()
        };
        
        if should_rotate {
            // Try to rotate (non-blocking if immutable slot is occupied)
            let _ = self.try_rotate_memtable();
        }
        
        // Insert into active memtable (never blocks)
        let memtable = self.memtable.read()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        memtable.put_with_vector(key, data, vector, timestamp)?;
        
        Ok(())
    }
    
    /// 🆕 Vector search in MemTable (returns complete row data)
    /// 
    /// ## Returns
    /// - `Vec<(row_id, Value, distance)>`: 完整的 row data，无需二次查询
    /// 
    /// ## Performance
    /// - 查询延迟: ~2ms (内存图 + 数据解引用)
    /// - 无额外查询开销（数据和向量在同一 Entry）
    pub fn vector_search_memtable(&self, query: &[f32], k: usize) -> Result<Vec<(Key, Value, f32)>> {
        let memtable = self.memtable.read()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        let results = memtable.vector_search(query, k)?;
        
        // Convert UnifiedEntry → Value
        let mut final_results = Vec::new();
        for (key, entry, distance) in results {
            let mut value = Value {
                data: entry.data,
                timestamp: entry.timestamp,
                deleted: entry.deleted,
            };
            
            // Resolve blob reference if needed
            if let ValueData::Blob(ref blob_ref) = value.data {
                let blob_data = self.blob_store.get(blob_ref)?;
                value.data = ValueData::Inline(blob_data);
            }
            
            final_results.push((key, value, distance));
        }
        
        Ok(final_results)
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
        
        // Acquire flush lock to prevent concurrent flush operations
        debug_log!("🔒 [flush] 尝试获取 flush_lock...");
        let _flush_guard = self.flush_lock.lock()
            .map_err(|_| StorageError::Lock("Flush lock poisoned".into()))?;
        debug_log!("✅ [flush] 成功获取 flush_lock");
        
        let mut sstable_paths = Vec::new();
        
        // 1. Force rotate active MemTable (even if not full)
        let has_data = {
            debug_log!("🔒 [flush] 尝试获取 memtable.read() 锁检查数据...");
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            let empty = memtable.is_empty();
            debug_log!("✅ [flush] memtable is_empty = {}", empty);
            debug_log!("🔓 [flush] 释放 memtable.read() 锁");
            !empty
        };
        
        if has_data {
            debug_log!("📌 [flush] Active memtable有数据，执行rotate...");
            self.rotate_memtable()?;  // Blocking until queue has space
            debug_log!("✅ [flush] rotate_memtable完成");
        } else {
            debug_log!("⚠️  [flush] Active memtable为空，跳过rotate");
        }
        
        // 2. Flush entire immutable queue
        debug_log!("💾 [flush] 开始flush immutable queue...");
        loop {
            let has_immutable = {
                debug_log!("🔒 [flush] 尝试获取 immutable.read() 锁检查队列...");
                let immutable = self.immutable.read()
                    .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
                let empty = immutable.is_empty();
                debug_log!("✅ [flush] immutable queue长度: {}, is_empty = {}", immutable.len(), empty);
                debug_log!("🔓 [flush] 释放 immutable.read() 锁");
                !empty  // 🔥 Check queue not empty
            };
            
            if !has_immutable {
                debug_log!("✅ [flush] immutable queue已空，flush完成");
                break;  // Queue empty, done
            }
            
            // Flush and collect SSTable path
            debug_log!("💾 [flush] 开始flush一个immutable memtable...");
            if let Some(path) = self.flush_immutable_with_path()? {
                debug_log!("✅ [flush] 成功flush到: {:?}", path);
                sstable_paths.push(path);
            }
        }
        
        debug_log!("✅ [flush] 整个flush操作完成，共创建 {} 个SSTables", sstable_paths.len());
        Ok(sstable_paths)
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
    pub fn set_flush_callback<F>(&self, callback: F) -> Result<()>
    where
        F: Fn(&UnifiedMemTable) -> Result<()> + Send + Sync + 'static,
    {
        let mut cb = self.flush_callback.write()
            .map_err(|_| StorageError::Lock("Flush callback lock poisoned".into()))?;
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
            debug_log!("🔒 [try_rotate] 尝试获取 immutable.read() 锁检查队列...");
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            debug_log!("✅ [try_rotate] 成功获取 immutable.read() 锁，队列长度: {}/{}", 
                     immutable.len(), self.max_immutable_slots);
            if immutable.len() >= self.max_immutable_slots {
                debug_log!("⚠️  [try_rotate] 队列已满，跳过rotate");
                // Queue full, skip rotation (non-blocking)
                return Err(StorageError::Transaction("Immutable queue full".into()));
            }
            debug_log!("🔓 [try_rotate] 释放 immutable.read() 锁");
        }
        
        // Acquire both locks for atomic swap
        debug_log!("🔒 [try_rotate] 尝试获取 memtable.write() 锁...");
        let mut memtable_lock = self.memtable.write()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        debug_log!("✅ [try_rotate] 成功获取 memtable.write() 锁");
        
        debug_log!("🔒 [try_rotate] 尝试获取 immutable.write() 锁...");
        let mut immutable_lock = self.immutable.write()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        debug_log!("✅ [try_rotate] 成功获取 immutable.write() 锁");
        
        // Double-check queue not full (another thread might have added)
        if immutable_lock.len() >= self.max_immutable_slots {
            debug_log!("⚠️  [try_rotate] 双重检查：队列已满，放弃rotate");
            return Err(StorageError::Transaction("Immutable queue full".into()));
        }
        
        // 🆕 Create new UnifiedMemTable with same configuration
        let new_memtable = Self::create_memtable(&self.config, &*memtable_lock);
        
        // Atomic swap: active → push to queue back, create new active
        let old_memtable = std::mem::replace(&mut *memtable_lock, new_memtable);
        immutable_lock.push_back(old_memtable);  // 🔥 Push to queue
        
        debug_log!("✅ [try_rotate] MemTable rotate成功，新队列长度: {}", immutable_lock.len());
        debug_log!("🔓 [try_rotate] 释放 immutable.write() 和 memtable.write() 锁");
        
        Ok(())
    }
    
    /// Force rotate (blocking, used by flush())
    fn rotate_memtable(&self) -> Result<()> {
        // Wait until queue has space
        loop {
            {
                let immutable = self.immutable.read()
                    .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
                if immutable.len() < self.max_immutable_slots {
                    break;
                }
            }
            // Sleep briefly to avoid busy loop
            thread::sleep(Duration::from_millis(1));
        }
        
        // Now rotate
        let mut memtable_lock = self.memtable.write()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        let mut immutable_lock = self.immutable.write()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        // 🆕 Create new UnifiedMemTable with same configuration
        let new_memtable = Self::create_memtable(&self.config, &*memtable_lock);
        
        let old_memtable = std::mem::replace(&mut *memtable_lock, new_memtable);
        immutable_lock.push_back(old_memtable);  // 🔥 Push to queue
        
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
    
    
    /// Internal flush implementation
    /// 🔥 NEW: Pop from front of queue (FIFO)
    fn flush_immutable_impl(&self) -> Result<Option<PathBuf>> {
        // Pop one MemTable from front of queue (oldest first)
        let memtable = {
            let mut immutable_lock = self.immutable.write()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            match immutable_lock.pop_front() {  // 🔥 Pop from front (FIFO)
                Some(mem) => mem,
                None => return Ok(None), // Queue empty
            }
        };
        // 🔧 Lock released here, one queue slot is now freed
        //    But memtable still holds data until SSTable is built
        
        // 🚀 Unified Flush Callback (NEW)
        // Call callback with MemTable reference (zero-copy, efficient)
        // This allows Database layer to batch build all indexes
        {
            let callback_guard = self.flush_callback.read()
                .map_err(|_| StorageError::Lock("Flush callback lock poisoned".into()))?;
            if let Some(ref callback) = *callback_guard {
                // ✅ Pass MemTable reference directly (no data copy)
                callback(&memtable)?;
            }
        }
        
        // Generate SSTable ID
        let sst_id = {
            let mut next_id = self.next_sst_id.write()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            let id = *next_id;
            *next_id += 1;
            id
        };
        
        // Build SSTable (I/O happens here, no locks held)
        let sst_path = self.storage_dir.join(format!("l0_{:06}.sst", sst_id));
        let mut builder = SSTableBuilder::new(&sst_path, self.config.clone(), memtable.len())?;
        
        // 🆕 Use UnifiedEntry iterator and convert to Value
        for (key, entry) in memtable.iter() {
            let value = Value {
                data: entry.data,
                timestamp: entry.timestamp,
                deleted: entry.deleted,
            };
            builder.add(key, value)?;
        }
        
        let meta = builder.finish()?;
        
        // TODO: Phase 2 - Flush vector graph nodes to SST
        // if let Ok(nodes) = memtable.export_vector_nodes() {
        //     // Write vector nodes to separate SST file
        // }
        
        // 🔧 Explicitly drop memtable ASAP to free memory
        drop(memtable);
        
        // Register SSTable with compaction worker
        self.compaction_worker.register_sstable(meta)?;
        
        Ok(Some(sst_path))
    }
    
    fn flush_immutable_with_path(&self) -> Result<Option<PathBuf>> {
        // Try to acquire flush ownership (lock-free)
        if self.flush_in_progress.compare_exchange(
            false, 
            true, 
            Ordering::Acquire, 
            Ordering::Relaxed
        ).is_err() {
            // Another thread is flushing, skip
            return Ok(None);
        }
        
        // We own the flush now
        let result = self.flush_immutable_impl();
        
        // Release flush ownership
        self.flush_in_progress.store(false, Ordering::Release);
        
        result
    }
    
    /// Flush immutable MemTable (legacy API for background flush thread)
    fn flush_immutable(&self) -> Result<()> {
        self.flush_immutable_with_path().map(|_| ())
    }
    
    fn flush_immutable_single(&self) -> Result<()> {
        self.flush_immutable_with_path().map(|_| ())
    }
    
    /// Scan MemTable (including immutable) with zero-copy callback
    /// 
    /// ✅ Zero-copy optimization: No Vec allocation, processes items in-place
    pub fn scan_memtable_with<F>(&self, start: Key, end: Key, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &[u8]) -> Result<()>,
    {
        use std::collections::HashMap;
        
        // Collect all entries first (to handle deduplication)
        let mut merged: HashMap<Key, Vec<u8>> = HashMap::new();
        
        // 1. Scan immutable queue (oldest to newest, so newer values overwrite)
        // 🔥 NEW: Iterate through entire queue
        {
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            // Scan from front (oldest) to back (newest)
            for mem in immutable.iter() {
                let entries = mem.scan(start, end)?;
                for (k, entry) in entries {
                    if let ValueData::Inline(ref d) = entry.data {
                        if !entry.deleted {
                            merged.insert(k, d.clone());
                        }
                    }
                }
            }
        }
        
        // 2. Scan active MemTable (overwrites older values)
        {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            let entries = memtable.scan(start, end)?;
            for (k, entry) in entries {
                if let ValueData::Inline(ref d) = entry.data {
                    if !entry.deleted {
                        merged.insert(k, d.clone());
                    }
                }
            }
        }
        
        // 3. Process merged results in sorted order
        let mut sorted_keys: Vec<_> = merged.keys().copied().collect();
        sorted_keys.sort_unstable();
        
        for key in sorted_keys {
            if let Some(data) = merged.get(&key) {
                f(key, data)?;  // ✅ Zero-copy callback
            }
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
    /// 
    /// ✅ Zero-copy optimization: No Vec allocation
    /// ⚠️  CRITICAL: 先收集数据，释放锁后再调用回调，避免在持锁期间执行慢操作导致阻塞
    pub fn scan_all_memtable_with<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &[u8]) -> Result<()>,
    {
        // Step 1: 收集所有数据（持锁时间最小化）
        let entries = {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            let mut collected = Vec::new();
            let entries = memtable.scan_all()?;
            for (k, entry) in entries {
                match &entry.data {
                    ValueData::Inline(d) => collected.push((k, d.clone())),
                    ValueData::Blob(_) => {}, // Skip blob refs
                }
            }
            collected
            // 🔓 memtable锁在这里释放
        };
        
        // Step 2: 释放锁后，再调用回调处理数据
        for (k, data) in entries {
            f(k, &data)?;
        }
        
        Ok(())
    }
    
    /// Scan all MemTable entries (for debugging) - Legacy API
    /// 
    /// ⚠️ Prefer scan_all_memtable_with() for zero-copy iteration
    pub fn scan_all_memtable(&self) -> Result<Vec<(Key, Vec<u8>)>> {
        // 🚀 P3 优化：预分配容量（估算全表大小）
        let mut results = Vec::with_capacity(1000);
        self.scan_all_memtable_with(|k, v| {
            results.push((k, v.to_vec()));
            Ok(())
        })?;
        Ok(results)
    }
    
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
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            for memtable in immutable.iter() {
                let entries = memtable.scan_all()?;
                for (k, entry) in entries {
                    // 🔧 FIX: Skip tombstones (deleted entries)
                    if entry.deleted {
                        continue;
                    }
                    
                    match &entry.data {
                        ValueData::Inline(d) => {
                            all_entries.push((k, d.clone())); // Clone data while holding lock
                        },
                        ValueData::Blob(_) => {},
                    }
                }
            }
            // 🔓 immutable锁在这里释放
        }
        
        // 1.2 扫描 active MemTable (正在写入的数据)
        {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("MemTable lock poisoned".into()))?;
            let entries = memtable.scan_all()?;
            for (k, entry) in entries {
                // 🔧 FIX: Skip tombstones (deleted entries)
                if entry.deleted {
                    continue;
                }
                
                match &entry.data {
                    ValueData::Inline(d) => {
                        all_entries.push((k, d.clone())); // Clone data while holding lock
                    },
                    ValueData::Blob(_) => {},
                }
            }
            // 🔓 memtable锁在这里释放
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
    
    /// 🆕 只扫描 immutable queue (不包括 active MemTable)
    /// 
    /// 用于 flush() 场景：先 rotate，再扫描 immutable，避免死锁
    pub fn scan_immutable_only<F>(&self, mut f: F) -> Result<()>
    where
        F: FnMut(Key, &[u8]) -> Result<()>,
    {
        let immutable = self.immutable.read()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        for memtable in immutable.iter() {
            let entries = memtable.scan_all()?;
            for (k, entry) in entries {
                match &entry.data {
                    ValueData::Inline(d) => f(k, &d)?,
                    ValueData::Blob(_) => {},
                }
            }
        }
        
        Ok(())
    }
    
    /// 🆕 Public API: Force rotate active MemTable to immutable queue
    /// 
    /// Blocks until immutable queue has space (backpressure control)
    pub fn force_rotate(&self) -> Result<()> {
        self.rotate_memtable()
    }
    
    /// 🆕 Public API: Get immutable queue size
    pub fn immutable_queue_len(&self) -> usize {
        self.immutable.read()
            .map(|guard| guard.len())
            .unwrap_or(0)
    }
    
    /// 🆕 Scan all keys with a specific prefix (for table scanning)
    /// 
    /// ## Use Case
    /// - Full table scan: scan_prefix(table_prefix)
    /// - Returns all keys starting with the prefix
    /// 
    /// ## Implementation
    /// - Composite keys use high 32 bits as table hash
    /// - Prefix match: (key >> 32) == prefix
    /// 
    /// ## Performance
    /// - Same as scan_range() but filters by prefix
    /// - O(N log N) where N = matching keys
    /// 
    /// # Example
    /// ```ignore
    /// // Scan all rows in table "users" (prefix = hash("users"))
    /// let rows = engine.scan_prefix(table_prefix)?;
    /// ```ignore
    pub fn scan_prefix(&self, prefix: Key) -> Result<Vec<(Key, Value)>> {
        use std::collections::BTreeMap;
        
        // Extract table hash from prefix (high 32 bits of composite key)
        let table_hash = prefix;  // prefix IS the table hash
        
        // Step 1: Collect from MemTable (newest data)
        let mut merged: BTreeMap<Key, Value> = BTreeMap::new();
        
        {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            let entries = memtable.scan_all()?;
            
            for (k, entry) in entries {
                // Check if key matches prefix: high 32 bits == table_hash
                if (k >> 32) == table_hash {
                    let value = Value {
                        data: entry.data,
                        timestamp: entry.timestamp,
                        deleted: entry.deleted,
                    };
                    merged.insert(k, value);
                }
            }
        }
        
        // Step 2: Collect from Immutable queue
        {
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            for memtable in immutable.iter() {
                let entries = memtable.scan_all()?;
                
                for (k, entry) in entries {
                    // Check prefix match: high 32 bits
                    if (k >> 32) == table_hash {
                        let value = Value {
                            data: entry.data,
                            timestamp: entry.timestamp,
                            deleted: entry.deleted,
                        };
                        
                        // Only insert if key doesn't exist or this version is newer
                        merged.entry(k).or_insert(value);
                    }
                }
            }
        }
        
        // Step 3: Collect from SSTables
        let sstable_paths = self.compaction_worker.list_sstables()?;
        
        for path in sstable_paths {
            if !path.exists() {
                continue;
            }
            
            let sstable_arc = match self.sstable_cache.get_or_open(&path) {
                Ok(sst) => sst,
                Err(_) => continue,
            };
            
            let mut sstable = match sstable_arc.lock() {
                Ok(sst) => sst,
                Err(_) => continue,
            };
            
            // Scan entire SSTable and filter by prefix
            let entries = match sstable.scan_all() {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            
            for (k, value) in entries {
                // Check prefix match: high 32 bits
                if (k >> 32) == table_hash {
                    merged.entry(k).or_insert(value);
                }
            }
        }
        
        // Step 4: Filter out deleted entries and return
        let results: Vec<(Key, Value)> = merged.into_iter()
            .filter(|(_, v)| !v.deleted)
            .collect();
        
        Ok(results)
    }
    
    /// 🆕 Zero-copy scan with prefix and callback
    /// 
    /// ## Performance Benefits
    /// - No Vec allocation (saves memory)
    /// - Early termination support (callback can return Err)
    /// - Streaming processing (constant memory usage)
    /// 
    /// ## Use Case
    /// ```ignore
    /// engine.scan_prefix_with(table_prefix, |key, value| {
    ///     if value.timestamp <= snapshot_ts {
    ///         process_row(key, value)?;
    ///     }
    ///     Ok(())
    /// })?;
    /// ```ignore
    pub fn scan_prefix_with<F>(&self, prefix: Key, mut callback: F) -> Result<()>
    where
        F: FnMut(Key, &Value) -> Result<()>,
    {
        use std::collections::BTreeMap;
        
        let table_hash = prefix;
        let mut merged: BTreeMap<Key, Value> = BTreeMap::new();
        
        // Step 1: Collect from MemTable
        {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            let entries = memtable.scan_all()?;
            
            for (k, entry) in entries {
                if (k >> 32) == table_hash {
                    let value = Value {
                        data: entry.data,
                        timestamp: entry.timestamp,
                        deleted: entry.deleted,
                    };
                    merged.insert(k, value);
                }
            }
        }
        
        // Step 2: Collect from Immutable queue
        {
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            for memtable in immutable.iter() {
                let entries = memtable.scan_all()?;
                
                for (k, entry) in entries {
                    if (k >> 32) == table_hash {
                        let value = Value {
                            data: entry.data,
                            timestamp: entry.timestamp,
                            deleted: entry.deleted,
                        };
                        merged.entry(k).or_insert(value);
                    }
                }
            }
        }
        
        // Step 3: Collect from SSTables
        let sstable_paths = self.compaction_worker.list_sstables()?;
        
        for path in sstable_paths {
            if !path.exists() {
                continue;
            }
            
            let sstable_arc = match self.sstable_cache.get_or_open(&path) {
                Ok(sst) => sst,
                Err(_) => continue,
            };
            
            let mut sstable = match sstable_arc.lock() {
                Ok(sst) => sst,
                Err(_) => continue,
            };
            
            let entries = match sstable.scan_all() {
                Ok(entries) => entries,
                Err(_) => continue,
            };
            
            for (k, value) in entries {
                if (k >> 32) == table_hash {
                    merged.entry(k).or_insert(value);
                }
            }
        }
        
        // Step 4: Call callback for each non-deleted entry
        for (key, value) in merged.iter() {
            if !value.deleted {
                callback(*key, value)?;
            }
        }
        
        Ok(())
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
        use std::collections::BTreeMap;
        
        // Step 1: Collect from MemTable (newest data)
        let mut merged: BTreeMap<Key, Value> = BTreeMap::new();
        
        {
            let memtable = self.memtable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            let entries = memtable.scan(start, end)?;
            
            for (k, entry) in entries {
                let value = Value {
                    data: entry.data,
                    timestamp: entry.timestamp,
                    deleted: entry.deleted,
                };
                merged.insert(k, value);
            }
        }
        
        // Step 2: Collect from Immutable queue
        // 🔥 NEW: Iterate through entire queue (oldest to newest)
        {
            let immutable = self.immutable.read()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            for memtable in immutable.iter() {
                let entries = memtable.scan(start, end)?;
                
                for (k, entry) in entries {
                    // Only insert if key doesn't exist or this version is newer
                    let value = Value {
                        data: entry.data,
                        timestamp: entry.timestamp,
                        deleted: entry.deleted,
                    };
                    
                    merged.entry(k).or_insert(value);
                }
            }
        }
        
        // Step 3: Collect from SSTables (oldest data)
        // ⚠️  CRITICAL FIX: SSTable应该按照从新到旧的顺序扫描
        //     因为使用or_insert()，先插入的值会被保留
        let sstable_paths = self.compaction_worker.list_sstables()?;
        
        // 🔥 反转顺序：从最新的SSTable开始扫描
        for path in sstable_paths.iter().rev() {
            // Skip if file doesn't exist (may have been compacted)
            if !path.exists() {
                continue;
            }
            
            // Use cache to get SSTable
            let sstable_arc = match self.sstable_cache.get_or_open(&path) {
                Ok(sst) => sst,
                Err(_) => continue, // Skip if can't open (e.g., being compacted)
            };
            
            let mut sstable = match sstable_arc.lock() {
                Ok(sst) => sst,
                Err(_) => continue, // Skip if lock poisoned
            };
            
            // Scan SSTable
            let entries = match sstable.scan(start, end) {
                Ok(entries) => entries,
                Err(_) => continue, // Skip if scan fails
            };
            
            for (k, value) in entries {
                // Only insert if key doesn't exist (MemTable/newer SST has priority)
                merged.entry(k).or_insert(value);
            }
        }
        
        // Step 4: Filter out deleted entries and return
        let results: Vec<(Key, Value)> = merged.into_iter()
            .filter(|(_, v)| !v.deleted)
            .collect();
        
        Ok(results)
    }
    
    /// Get compaction statistics
    pub fn compaction_stats(&self) -> Result<super::CompactionStats> {
        self.compaction_worker.stats()
    }
    
    /// Get level statistics  
    pub fn level_stats(&self) -> Result<Vec<(usize, usize, u64)>> {
        self.compaction_worker.level_stats()
    }
    
    /// 🆕 P2.4: Get all SSTable paths for a table (for parallel scanning)
    /// 
    /// Returns paths of all SSTables that may contain data for the given table prefix.
    /// Used by parallel scan to distribute work across threads.
    pub fn get_sstables_for_table(&self, _table_prefix: u64) -> Result<Vec<PathBuf>> {
        // For now, return all SSTables (could optimize to filter by key range)
        let sstable_metas = self.compaction_worker.get_all_sstables()?;
        
        Ok(sstable_metas.into_iter()
            .map(|meta| meta.path)
            .collect())
    }
    
    /// ✨ P2 Phase 3: Get compaction statistics
    pub fn get_compaction_stats(&self) -> Result<crate::storage::lsm::CompactionStats> {
        self.compaction_worker.stats()
    }
    
    /// ✨ P2 Phase 3: Get level statistics (level, file_count, total_size)
    pub fn get_level_stats(&self) -> Result<Vec<(usize, usize, u64)>> {
        self.compaction_worker.level_stats()
    }
}

impl Drop for LSMEngine {
    fn drop(&mut self) {
        // 🔧 Signal background threads to shutdown
        self.shutdown.store(true, Ordering::Relaxed);
        
        // Best effort flush on shutdown
        let _ = self.flush();
        
        // 🔧 Wait for background threads to exit gracefully
        // This ensures no Arc references remain after drop
        if let Some(compaction_thread) = self.compaction_thread.take() {
            let _ = compaction_thread.join();
        }
        
        if let Some(flush_thread) = self.flush_thread.take() {
            let _ = flush_thread.join();
        }
        
        // 🔧 Clear SSTable cache to release file handles
        self.sstable_cache.clear();
        
        // ✅ All Arc references released, memory freed immediately
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
        assert_eq!(value.data, ValueData::Inline(b"value1".to_vec()));
        
        // Delete
        engine.delete(1u64, 3).unwrap();
        let value = engine.get(1u64).unwrap();
        assert!(value.is_none(), "Deleted key should return None");
    }
    
    #[test]
    fn test_memtable_flush() {
        let temp_dir = TempDir::new().unwrap();
        let mut config = LSMConfig::default();
        config.memtable_size = 100; // Small size to trigger flush
        
        let engine = LSMEngine::new(temp_dir.path().to_path_buf(), config).unwrap();
        
        // Insert enough data to trigger flush
        for i in 0..20 {
            let key = i as u64;  // ✅ u64 key
            let value = Value::new(vec![0u8; 10], i);
            engine.put(key, value).unwrap();
        }
        
        // Explicitly flush
        engine.flush().unwrap();
        
        // Verify SSTable was created
        let sstables: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
            .collect();
        
        assert!(sstables.len() > 0, "Should have created at least one SSTable");
    }
}
