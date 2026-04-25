//! Leveled Compaction Strategy
//!
//! ## Algorithm
//! - L0: Overlapping files from MemTable flush
//! - L1-L6: Non-overlapping files, size-tiered
//! - Trigger: L(n) size > threshold
//! - Merge: L(n) + L(n+1) → L(n+1)
//!
//! ## Write Amplification
//! - Target: < 30x (RocksDB典型值)
//! - Calculation: bytes_written / bytes_inserted
//! - 优化: 减少层数、提高level_multiplier

use super::{SSTable, SSTableBuilder, LSMConfig, Key};
use super::bloom::BloomFilter;
use crate::{Result, StorageError};
use std::sync::Arc;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use parking_lot::RwLock;
use std::fs;

/// Compaction statistics
#[derive(Clone, Debug, Default)]
pub struct CompactionStats {
    /// Total bytes read
    pub bytes_read: u64,
    
    /// Total bytes written
    pub bytes_written: u64,
    
    /// Number of compactions
    pub num_compactions: u64,
    
    /// Write amplification factor
    pub write_amplification: f64,
    
    /// ✨ P2 Phase 3: Enhanced statistics
    /// L0 tiered compactions (L0.x → L0.y)
    pub tiered_compactions: u64,
    
    /// L0 → L1 full compactions
    pub l0_to_l1_compactions: u64,
    
    /// L1+ compactions
    pub levelplus_compactions: u64,
    
    /// Bytes saved by tiered strategy
    pub bytes_saved: u64,
}

/// Level metadata
#[derive(Clone, Debug)]
pub struct Level {
    /// Level number (0-6)
    pub level: usize,
    
    /// SSTable files in this level
    pub sstables: Vec<SSTableMeta>,
    
    /// Total size in bytes
    pub total_size: u64,
    
    /// Size threshold for compaction
    pub size_threshold: u64,
    
    /// L0 sublevel structure (for tiered compaction)
    /// Only used for level 0, None for L1+
    pub sublevels: Option<Vec<TieredSublevel>>,
}

/// L0 Tiered sublevel for reducing write amplification
/// 
/// ## Strategy
/// - L0.0: 0-2 files (newest data, from MemTable flush)
/// - L0.1: 3-5 files (intermediate tier)
/// - L0.2: 6-8 files (ready for L1 compaction)
#[derive(Clone, Debug)]
pub struct TieredSublevel {
    /// Sublevel index (0, 1, 2)
    pub sublevel: usize,
    
    /// SSTables in this sublevel
    pub sstables: Vec<SSTableMeta>,
    
    /// Max files before compacting to next sublevel
    pub max_files: usize,
}

/// SSTable metadata
#[derive(Clone, Debug)]
pub struct SSTableMeta {
    /// File path
    pub path: PathBuf,

    /// File size
    pub size: u64,

    /// Number of entries
    pub num_entries: u64,

    /// Min key
    pub min_key: Key,

    /// Max key
    pub max_key: Key,

    /// Min timestamp
    pub min_timestamp: u64,

    /// Max timestamp
    pub max_timestamp: u64,

    /// Bloom filter for lock-free pre-check (avoids SSTableCache mutex).
    /// Populated during flush/compaction; None for SSTables discovered at startup
    /// (loaded lazily on first access via SSTableCache).
    pub bloom_filter: Option<Arc<BloomFilter>>,
}

impl Level {
    /// Create a new level
    pub fn new(level: usize, config: &LSMConfig) -> Self {
        let base_size = 10 * 1024 * 1024; // L1: 10MB
        let size_threshold = if level == 0 {
            base_size // L0: 10MB
        } else {
            base_size * config.level_multiplier.pow(level as u32 - 1)
        } as u64;
        
        // Initialize L0 sublevels for tiered compaction
        let sublevels = if level == 0 {
            Some(vec![
                TieredSublevel { sublevel: 0, sstables: Vec::new(), max_files: 2 },  // L0.0
                TieredSublevel { sublevel: 1, sstables: Vec::new(), max_files: 3 },  // L0.1
                TieredSublevel { sublevel: 2, sstables: Vec::new(), max_files: 3 },  // L0.2
            ])
        } else {
            None
        };
        
        Self {
            level,
            sstables: Vec::new(),
            total_size: 0,
            size_threshold,
            sublevels,
        }
    }
    
    /// Add an SSTable to this level
    pub fn add_sstable(&mut self, meta: SSTableMeta) {
        self.total_size += meta.size;
        
        // For L0 with tiered compaction, add to sublevel 0
        if self.level == 0 && self.sublevels.is_some() {
            if let Some(ref mut sublevels) = self.sublevels {
                sublevels[0].sstables.push(meta.clone());
            }
        }
        
        // Also add to main sstables list for query (legacy compatibility)
        self.sstables.push(meta);
        
        // Sort by min_key for L1+ (L0 can overlap)
        if self.level > 0 {
            self.sstables.sort_by(|a, b| a.min_key.cmp(&b.min_key));
        }
    }
    
    /// Remove an SSTable
    pub fn remove_sstable(&mut self, path: &Path) {
        if let Some(idx) = self.sstables.iter().position(|s| s.path == path) {
            let meta = self.sstables.remove(idx);
            self.total_size = self.total_size.saturating_sub(meta.size);
        }
        // Also remove from sublevels (L0 tiered compaction metadata)
        if self.level == 0 {
            if let Some(ref mut sublevels) = self.sublevels {
                for sublevel in sublevels.iter_mut() {
                    if let Some(idx) = sublevel.sstables.iter().position(|s| s.path == path) {
                        sublevel.sstables.remove(idx);
                    }
                }
            }
        }
    }
    
    /// Check if compaction is needed
    /// 
    /// 🚀 P1 优化：更激进的 L0 compaction 触发策略
    /// - L0: 2 个文件就触发（原 4 个）
    /// - 目标：将 L0 SSTable 数量从 425 降低到 < 10
    pub fn needs_compaction(&self) -> bool {
        if self.level == 0 {
            // Check L0 tiered sublevels first
            if let Some(ref sublevels) = self.sublevels {
                // 🔥 P1: 降低 sublevel 阈值
                for sublevel in sublevels {
                    if sublevel.sstables.len() >= 2 {  // 🚀 降低：max_files → 2
                        return true;
                    }
                }
                return false;
            }
            
            // Fallback: L0 trigger by file count (legacy)
            self.sstables.len() >= 2  // 🚀 P1: 降低阈值 4 → 2
        } else {
            // L1+: trigger by total size
            self.total_size > self.size_threshold
        }
    }
    
    /// Check which L0 sublevel needs compaction
    pub fn get_sublevel_to_compact(&self) -> Option<usize> {
        if self.level != 0 {
            return None;
        }
        
        if let Some(ref sublevels) = self.sublevels {
            // Check sublevels in order (0 → 1 → 2)
            for sublevel in sublevels {
                if sublevel.sstables.len() >= sublevel.max_files {
                    return Some(sublevel.sublevel);
                }
            }
        }
        
        None
    }
    
    /// Select SSTables for compaction
    pub fn select_for_compaction(&self, config: &LSMConfig) -> Vec<SSTableMeta> {
        if self.level == 0 {
            // Use all L0 SSTables for regular compaction.
            // Tiered sublevel selection is disabled due to a data-loss bug.
            self.sstables.clone()
        } else {
            // L1+: select oldest/largest files
            let mut candidates = self.sstables.clone();
            candidates.sort_by(|a, b| b.size.cmp(&a.size));
            candidates.truncate(config.l0_compaction_trigger);
            candidates
        }
    }
    
    /// Get overlapping SSTables in next level
    /// 🚀 P3 优化：预分配容量
    pub fn get_overlapping(&self, next_level: &Level, sources: &[SSTableMeta]) -> Vec<SSTableMeta> {
        if sources.is_empty() {
            return Vec::new();
        }
        
        let min_key = sources.iter().map(|s| &s.min_key).min().unwrap();
        let max_key = sources.iter().map(|s| &s.max_key).max().unwrap();
        
        // 🚀 预分配容量（估算重叠数量）
        let mut overlapping = Vec::with_capacity(next_level.sstables.len() / 2);
        
        for sst in &next_level.sstables {
            // Check if [min_key, max_key] overlaps with [sst.min_key, sst.max_key]
            if &sst.min_key <= max_key && &sst.max_key >= min_key {
                overlapping.push(sst.clone());
            }
        }
        
        overlapping
    }
}

/// Compaction configuration
#[derive(Clone, Debug)]
pub struct CompactionConfig {
    pub lsm_config: LSMConfig,
}

/// Type alias to reduce complexity of the post-compaction callback type.
type PostCompactionCbFn = Box<dyn Fn(&[PathBuf]) + Send + Sync>;
type PostCompactionCb = Arc<std::sync::RwLock<Option<PostCompactionCbFn>>>;

/// Compaction worker
pub struct CompactionWorker {
    /// Storage directory
    storage_dir: PathBuf,

    /// Level metadata
    levels: Arc<Mutex<Vec<Level>>>,

    /// Configuration
    config: CompactionConfig,

    /// Statistics
    stats: Arc<Mutex<CompactionStats>>,

    /// Callback invoked after compaction replaces SSTables.
    /// Receives the paths of SSTables that were removed, so the cache can
    /// evict only those entries instead of clearing entirely.
    post_compaction_cb: PostCompactionCb,

    /// SSTable files pending deletion from a previous compaction cycle.
    /// Deferred by one cycle so in-flight scans finish before files are removed.
    pending_deletions: Mutex<Vec<PathBuf>>,

    /// Cached snapshot of all SSTable metadata
    /// Readers access this via cheap Arc clone (no Mutex contention).
    /// Updated atomically after register_sstable() and run_compaction().
    sstable_snapshot: RwLock<Option<Arc<Vec<SSTableMeta>>>>,

    /// Shared epoch counter (bumped on compaction) so scans can detect SSTable changes.
    compaction_epoch: Arc<std::sync::atomic::AtomicU64>,
}

impl CompactionWorker {
    /// Create a new compaction worker
    pub fn new(storage_dir: PathBuf, config: &LSMConfig) -> Self {
        let mut levels = Vec::new();
        for level in 0..config.num_levels {
            levels.push(Level::new(level, config));
        }

        let worker = Self {
            storage_dir,
            levels: Arc::new(Mutex::new(levels)),
            config: CompactionConfig {
                lsm_config: config.clone(),
            },
            stats: Arc::new(Mutex::new(CompactionStats::default())),
            post_compaction_cb: Arc::new(std::sync::RwLock::new(None)),
            pending_deletions: Mutex::new(Vec::new()),
            sstable_snapshot: RwLock::new(None),
            compaction_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };

        // Discover existing SSTables on disk
        if let Err(e) = worker.discover_sstables() {
            debug_log!("[CompactionWorker] Warning: failed to discover SSTables: {:?}", e);
        }

        worker
    }

    /// Discover existing .sst files in the storage directory and register them.
    /// Called during startup so that previously flushed data is visible to scans.
    fn discover_sstables(&self) -> Result<()> {
        let entries = match std::fs::read_dir(&self.storage_dir) {
            Ok(e) => e,
            Err(_) => return Ok(()), // Directory doesn't exist yet — nothing to discover
        };

        let mut discovered: Vec<(usize, SSTableMeta)> = Vec::new();

        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) == Some("sst") {
                // Parse level from filename: "l{level}_*.sst"
                let file_name = path.file_stem().and_then(|n| n.to_str()).unwrap_or("");
                let level = if let Some(rest) = file_name.strip_prefix('l') {
                    rest.split('_').next()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0)
                } else {
                    0
                };

                // Read metadata with real min/max keys from index block
                match crate::storage::lsm::sstable::SSTable::read_metadata_with_keys(&path) {
                    Ok((num_entries, min_timestamp, file_size, min_key, max_key)) => {
                        let meta = SSTableMeta {
                            path: path.clone(),
                            size: file_size,
                            num_entries,
                            min_key,
                            max_key,
                            min_timestamp,
                            max_timestamp: min_timestamp,
                            bloom_filter: None,
                        };
                        discovered.push((level.min(self.config.lsm_config.num_levels - 1), meta));
                    }
                    Err(e) => {
                        // Fall back to read_metadata without keys (corrupt index but valid footer)
                        debug_log!("[CompactionWorker] Warning: failed to read keys from {:?}: {:?}, trying footer-only", path, e);
                        match crate::storage::lsm::sstable::SSTable::read_metadata(&path) {
                            Ok((num_entries, min_timestamp, file_size)) => {
                                let meta = SSTableMeta {
                                    path: path.clone(),
                                    size: file_size,
                                    num_entries,
                                    min_key: 0,
                                    max_key: u64::MAX,
                                    min_timestamp,
                                    max_timestamp: min_timestamp,
                                    bloom_filter: None,
                                };
                                discovered.push((level.min(self.config.lsm_config.num_levels - 1), meta));
                            }
                            Err(e2) => {
                                debug_log!("[CompactionWorker] Warning: skipping corrupt SSTable {:?}: {:?}", path, e2);
                            }
                        }
                    }
                }
            }
        }

        if !discovered.is_empty() {
            debug_log!("[CompactionWorker] Discovered {} existing SSTables", discovered.len());
            let mut levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

            for (level, meta) in discovered {
                levels[level].add_sstable(meta);
            }
        }

        Ok(())
    }

    /// Set a callback to invoke after compaction replaces SSTables.
    /// The callback receives the paths of SSTables that were removed by compaction.
    /// Used to selectively invalidate SSTableCache entries.
    pub fn set_post_compaction_cb(&self, cb: PostCompactionCbFn) {
        let mut guard = self.post_compaction_cb.write().unwrap();
        *guard = Some(cb);
    }

    /// Invoke the post-compaction callback (if set) with the removed SSTable paths.
    fn invoke_post_compaction(&self, removed_paths: &[PathBuf]) {
        if let Ok(guard) = self.post_compaction_cb.read() {
            if let Some(ref cb) = guard.as_ref() {
                cb(removed_paths);
            }
        }
    }

    /// Delete SST files deferred from a previous compaction cycle.
    /// Called at the start of each compaction so in-flight scans from the
    /// last cycle have finished by now.
    pub fn flush_pending_deletions(&self) {
        let pending = {
            let mut guard = self.pending_deletions.lock()
                .unwrap_or_else(|e| e.into_inner());
            std::mem::take(&mut *guard)
        };
        for path in &pending {
            let _ = fs::remove_file(path);
        }
    }

    /// Defer file deletion to the next compaction cycle instead of deleting now.
    fn defer_deletion(&self, path: PathBuf) {
        let mut guard = self.pending_deletions.lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.push(path);
    }
    
    /// Register a new SSTable (from MemTable flush)
    pub fn register_sstable(&self, meta: SSTableMeta) -> Result<()> {
        let mut levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

        // Always add to L0
        levels[0].add_sstable(meta);

        // 🚀 Invalidate snapshot so next read rebuilds it
        self.invalidate_snapshot();

        Ok(())
    }
    
    /// Check if compaction is needed
    pub fn needs_compaction(&self) -> Result<bool> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        Ok(levels.iter().any(|level| level.needs_compaction()))
    }
    
    /// Get all SSTables across all levels (for query)
    ///
    /// 🚀 Returns Arc clone (O(1)) instead of cloning the entire Vec.
    /// Uses cached snapshot to avoid Mutex on levels.
    pub fn get_all_sstables(&self) -> Result<Arc<Vec<SSTableMeta>>> {
        // Fast path: read cached snapshot (O(1) Arc clone, no Mutex on levels needed)
        {
            let snap = self.sstable_snapshot.read();
            if let Some(ref arc_vec) = *snap {
                return Ok(Arc::clone(arc_vec));
            }
        }

        // Slow path: build snapshot from levels and cache it
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

        let mut all_sstables = Vec::new();
        for level in levels.iter() {
            all_sstables.extend(level.sstables.iter().cloned());
        }

        // Cache for future reads
        let arc_vec = Arc::new(all_sstables);
        {
            let mut snap = self.sstable_snapshot.write();
            *snap = Some(Arc::clone(&arc_vec));
        }

        Ok(arc_vec)
    }

    /// 🚀 P0: Invalidate cached SSTable snapshot (called after register/run_compaction)
    fn invalidate_snapshot(&self) {
        let mut snap = self.sstable_snapshot.write();
        *snap = None;
    }
    
    /// Access the compaction epoch (for scan consistency checks)
    pub fn compaction_epoch(&self) -> &Arc<std::sync::atomic::AtomicU64> {
        &self.compaction_epoch
    }

    /// Run one round of compaction
    pub fn run_compaction(&self) -> Result<()> {
        // Flush deferred deletions from previous compaction cycle
        self.flush_pending_deletions();

        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        // Find first level that needs compaction
        let level_idx = match levels.iter().position(|l| l.needs_compaction()) {
            Some(idx) => idx,
            None => return Ok(()), // No compaction needed
        };
        
        // ✨ Special handling for L0 tiered compaction
        // DISABLED: Tiered compaction has a data-loss bug where incremental_merge batches
        // don't cross-reference overlapping SSTables in other sublevels, causing rows to be
        // silently dropped during merge. Falls through to the regular compaction path which
        // correctly handles all overlapping SSTables.
        // if level_idx == 0 {
        //     if let Some(sublevel_idx) = levels[0].get_sublevel_to_compact() {
        //         drop(levels);  // Release lock before I/O
        //         return self.run_tiered_compaction(sublevel_idx);
        //     }
        // }
        
        if level_idx >= levels.len() - 1 {
            return Ok(());  // Last level, can't compact further
        }
        
        // Select source files
        let sources = levels[level_idx].select_for_compaction(&self.config.lsm_config);
        let overlapping = levels[level_idx].get_overlapping(&levels[level_idx + 1], &sources);
        
        drop(levels); // Release lock during I/O
        
        // ✅ 检查文件是否存在
        let valid_sources: Vec<_> = sources.iter()
            .filter(|s| s.path.exists())
            .cloned()
            .collect();
        let valid_overlapping: Vec<_> = overlapping.iter()
            .filter(|s| s.path.exists())
            .cloned()
            .collect();
        
        if valid_sources.is_empty() && valid_overlapping.is_empty() {
            // 所有源文件都不存在（可能被并发compaction删除），跳过这次compaction
            // 清理元数据中的记录
            let mut levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            for source in &sources {
                levels[level_idx].remove_sstable(&source.path);
            }
            for overlap in &overlapping {
                levels[level_idx + 1].remove_sstable(&overlap.path);
            }
            
            return Ok(());
        }
        
        // Merge SSTables
        let output_meta = self.merge_sstables(level_idx + 1, &valid_sources, &valid_overlapping)?;
        
        // Update levels
        // Invalidate snapshot BEFORE modifying levels so that concurrent scans
        // don't get a stale cached snapshot with removed SSTables.
        self.invalidate_snapshot();
        let mut levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

        // Collect all removed SSTable paths for selective cache eviction
        let mut removed_paths: Vec<PathBuf> = Vec::with_capacity(sources.len() + overlapping.len());

        // Remove source files (deferred — actual deletion happens next compaction cycle)
        for source in &valid_sources {
            levels[level_idx].remove_sstable(&source.path);
            removed_paths.push(source.path.clone());
            self.defer_deletion(source.path.clone());
        }

        // Also clean up metadata for files that didn't exist
        for source in &sources {
            if !valid_sources.iter().any(|v| v.path == source.path) {
                levels[level_idx].remove_sstable(&source.path);
                removed_paths.push(source.path.clone());
            }
        }

        // Remove overlapping files (deferred)
        for overlap in &valid_overlapping {
            levels[level_idx + 1].remove_sstable(&overlap.path);
            removed_paths.push(overlap.path.clone());
            self.defer_deletion(overlap.path.clone());
        }

        // Also clean up metadata for files that didn't exist
        for overlap in &overlapping {
            if !valid_overlapping.iter().any(|v| v.path == overlap.path) {
                levels[level_idx + 1].remove_sstable(&overlap.path);
                removed_paths.push(overlap.path.clone());
            }
        }

        // Add output file
        levels[level_idx + 1].add_sstable(output_meta);

        // Update stats
        let mut stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        stats.num_compactions += 1;

        let bytes_read: u64 = valid_sources.iter().map(|s| s.size).sum::<u64>()
            + valid_overlapping.iter().map(|s| s.size).sum::<u64>();
        stats.bytes_read += bytes_read;

        // ✨ Track L1+ compaction stats
        if level_idx >= 1 {
            stats.levelplus_compactions += 1;
        }

        drop(stats);
        drop(levels);

        // 🚀 Invalidate snapshot so next read rebuilds it
        self.invalidate_snapshot();

        // Bump compaction epoch so in-flight scans detect SSTable changes
        self.compaction_epoch.fetch_add(1, std::sync::atomic::Ordering::Release);

        // Selectively evict only removed SSTables from cache (not a full clear)
        self.invoke_post_compaction(&removed_paths);

        Ok(())
    }
    
    /// Merge multiple SSTables into one
    /// 
    /// Note: sources and overlapping should already be filtered for existing files
    fn merge_sstables(
        &self,
        output_level: usize,
        sources: &[SSTableMeta],
        overlapping: &[SSTableMeta],
    ) -> Result<SSTableMeta> {
        let rate_limit = self.config.lsm_config.compaction_rate_limit.unwrap_or(u64::MAX);
        let yield_interval = self.config.lsm_config.compaction_yield_every_n_blocks;

        // Open ALL input SSTables. Skipping any input causes data loss because
        // run_compaction removes all source+overlapping SSTables from metadata
        // regardless of whether they were included in the merge output.
        let mut all_inputs = Vec::new();
        let all_sources: Vec<&SSTableMeta> = sources.iter().chain(overlapping.iter()).collect();

        for meta in &all_sources {
            match SSTable::open(&meta.path) {
                Ok(sstable) => all_inputs.push(sstable),
                Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug_log!("⚠️ SSTable disappeared during open: {:?}", meta.path);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        if all_inputs.is_empty() {
            return Err(StorageError::Index(
                "All input SSTables disappeared during compaction".into()
            ));
        }

        // Generate output file path
        let stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        let output_id = stats.num_compactions;
        let output_path = self.storage_dir.join(format!("l{}_{:06}.sst", output_level, output_id));
        drop(stats);

        // Streaming merge
        let mut iters: Vec<_> = all_inputs.into_iter()
            .filter_map(|mut sst| sst.iter().ok())
            .collect();

        if iters.is_empty() {
            return Err(StorageError::Index("No valid iterators for compaction".into()));
        }

        let estimated_size = sources.len() + overlapping.len() * 1000;
        let mut builder = SSTableBuilder::new(&output_path, self.config.lsm_config.clone(), estimated_size)?;

        // Multi-way merge-sort with priority queue
        use std::collections::BinaryHeap;

        #[derive(Debug, Clone)]
        struct MergeEntry {
            key: u64,
            value: super::Value,
            iter_idx: usize,
        }

        impl Eq for MergeEntry {}
        impl PartialEq for MergeEntry {
            fn eq(&self, other: &Self) -> bool {
                self.key == other.key && self.iter_idx == other.iter_idx
            }
        }

        impl Ord for MergeEntry {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                other.key.cmp(&self.key) // min-heap
            }
        }

        impl PartialOrd for MergeEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(MergeEntry { key, value, iter_idx: idx });
            }
        }

        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let tombstone_ttl_micros: u64 = 86_400 * 1_000_000;

        let mut last_key: Option<u64> = None;
        let mut last_value: Option<super::Value> = None;
        let mut entries_written: u64 = 0;
        let merge_start = std::time::Instant::now();
        let mut _bytes_written: u64 = 0;

        while let Some(entry) = heap.pop() {
            if Some(entry.key) == last_key {
                if let Some(ref mut last) = last_value {
                    if entry.value.timestamp > last.timestamp {
                        *last = entry.value;
                    }
                }
            } else {
                if let (Some(key), Some(value)) = (last_key, last_value.take()) {
                    if !value.deleted || (now_micros.saturating_sub(value.timestamp) < tombstone_ttl_micros) {
                        builder.add(key, value)?;
                        entries_written += 1;

                        // Throttle: rate limit + cooperative yield
                        if entries_written.is_multiple_of(100) {
                            // Estimate bytes written (rough: ~50B per entry)
                            _bytes_written = entries_written * 50;
                            let elapsed = merge_start.elapsed().as_secs_f64();
                            let expected = _bytes_written as f64 / rate_limit as f64;
                            if elapsed < expected {
                                std::thread::sleep(std::time::Duration::from_secs_f64(expected - elapsed));
                            }
                            // Cooperative yield every yield_interval * 100 entries
                            if (entries_written / 100).is_multiple_of(yield_interval as u64) {
                                std::thread::sleep(std::time::Duration::from_millis(1));
                            }
                        }
                    }
                }

                last_key = Some(entry.key);
                last_value = Some(entry.value);
            }

            if let Some((key, value)) = iters[entry.iter_idx].next() {
                heap.push(MergeEntry { key, value, iter_idx: entry.iter_idx });
            }
        }

        // Write final key
        if let (Some(key), Some(value)) = (last_key, last_value) {
            if !value.deleted || (now_micros.saturating_sub(value.timestamp) < tombstone_ttl_micros) {
                builder.add(key, value)?;
            }
        }

        let output_meta = builder.finish()?;

        let mut stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        stats.bytes_written += output_meta.size;
        if stats.bytes_read > 0 {
            stats.write_amplification = stats.bytes_written as f64 / stats.bytes_read as f64;
        }

        Ok(output_meta)
    }

    /// Get compaction statistics
    pub fn stats(&self) -> Result<CompactionStats> {
        let stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        Ok(stats.clone())
    }
    
    /// Get level statistics
    pub fn level_stats(&self) -> Result<Vec<(usize, usize, u64)>> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        Ok(levels.iter().map(|l| (l.level, l.sstables.len(), l.total_size)).collect())
    }
    
    /// List all SSTable paths (for range scan)
    /// Returns paths sorted by level (L0 first = newest)
    pub fn list_sstables(&self) -> Result<Vec<PathBuf>> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        let mut paths = Vec::new();
        
        // Collect from all levels (L0 first = newest data)
        for level in levels.iter() {
            for sst in &level.sstables {
                paths.push(sst.path.clone());
            }
        }
        
        Ok(paths)
    }
    
    /// ✨ P2 Phase 3: Run tiered compaction for L0 sublevels
    /// 
    /// This reduces write amplification by:
    /// - L0.0 → L0.1: Merge 2 files → 1 file (small, fast)
    /// - L0.1 → L0.2: Merge 3 files → 1 file (medium)
    /// - L0.2 → L1: Merge 3 files + overlapping L1 → L1 (full merge)
    #[allow(dead_code)]
    fn run_tiered_compaction(&self, sublevel_idx: usize) -> Result<()> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

        let sources = if let Some(ref sublevels) = levels[0].sublevels {
            if sublevel_idx >= sublevels.len() {
                return Ok(());
            }
            sublevels[sublevel_idx].sstables.clone()
        } else {
            return Ok(());  // No tiered structure
        };

        drop(levels);  // Release lock during I/O

        // ✅ Check file existence
        let valid_sources: Vec<_> = sources.iter()
            .filter(|s| s.path.exists())
            .cloned()
            .collect();
        
        if valid_sources.is_empty() {
            // Clean up metadata
            let mut levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            if let Some(ref mut sublevels) = levels[0].sublevels {
                if sublevel_idx < sublevels.len() {
                    sublevels[sublevel_idx].sstables.clear();
                }
            }
            
            return Ok(());
        }
        
        // Determine target: L0.{n+1} or L1
        let target_sublevel = sublevel_idx + 1;
        let compact_to_l1 = target_sublevel >= 3;  // L0.2 → L1
        
        // Collect all removed SSTable paths for selective cache eviction
        let mut removed_paths: Vec<PathBuf> = Vec::new();

        if compact_to_l1 {
            // Full compaction to L1 (include overlapping files)
            let levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

            let overlapping = levels[0].get_overlapping(&levels[1], &valid_sources);
            drop(levels);

            let valid_overlapping: Vec<_> = overlapping.iter()
                .filter(|s| s.path.exists())
                .cloned()
                .collect();

            // Merge to L1
            let output_meta = self.merge_sstables(1, &valid_sources, &valid_overlapping)?;

            // Update levels — invalidate snapshot first
            self.invalidate_snapshot();
            let mut levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

            // Remove from L0 sublevel
            if let Some(ref mut sublevels) = levels[0].sublevels {
                if sublevel_idx < sublevels.len() {
                    sublevels[sublevel_idx].sstables.clear();
                }
            }

            // Remove from L0 main list (deferred)
            for source in &valid_sources {
                levels[0].remove_sstable(&source.path);
                removed_paths.push(source.path.clone());
                self.defer_deletion(source.path.clone());
            }

            // Remove overlapping from L1 (deferred)
            for overlap in &valid_overlapping {
                levels[1].remove_sstable(&overlap.path);
                removed_paths.push(overlap.path.clone());
                self.defer_deletion(overlap.path.clone());
            }

            // Add to L1
            levels[1].add_sstable(output_meta);
        } else {
            // ✨ Incremental merge to next sublevel (P2 Phase 3.2)
            let output_metas = self.incremental_merge(&valid_sources, sublevel_idx)?;

            // Update sublevels — invalidate snapshot first
            self.invalidate_snapshot();
            let mut levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;

            // Remove from source sublevel and add to target sublevel
            if let Some(ref mut sublevels) = levels[0].sublevels {
                if sublevel_idx < sublevels.len() {
                    sublevels[sublevel_idx].sstables.clear();
                }

                // Add to target sublevel
                if target_sublevel < sublevels.len() {
                    for meta in &output_metas {
                        sublevels[target_sublevel].sstables.push(meta.clone());
                    }
                }
            }

            // Update main list — remove old sources, add merged outputs
            for source in &valid_sources {
                levels[0].remove_sstable(&source.path);
                removed_paths.push(source.path.clone());
                self.defer_deletion(source.path.clone());
            }
            for meta in output_metas {
                levels[0].sstables.push(meta);
            }
        }

        // Update stats
        let mut stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        stats.num_compactions += 1;

        let bytes_read: u64 = valid_sources.iter().map(|s| s.size).sum();
        stats.bytes_read += bytes_read;

        // ✨ Track tiered compaction stats
        if compact_to_l1 {
            stats.l0_to_l1_compactions += 1;
        } else {
            stats.tiered_compactions += 1;
            // Estimate bytes saved by delaying L1 compaction
            stats.bytes_saved += bytes_read;
        }

        // Update write amplification
        if stats.bytes_read > 0 {
            stats.write_amplification = stats.bytes_written as f64 / stats.bytes_read as f64;
        }

        drop(stats);

        // 🚀 Invalidate snapshot so next read rebuilds it
        self.invalidate_snapshot();

        // Selectively evict only removed SSTables from cache (not a full clear)
        self.invoke_post_compaction(&removed_paths);

        Ok(())
    }
    ///
    /// Instead of merging all N files at once:
    /// - Split into batches of 2
    /// - Merge each batch independently
    /// - Reduces single-merge data volume by 50%
    #[allow(dead_code)]
    fn incremental_merge(&self, sources: &[SSTableMeta], sublevel: usize) -> Result<Vec<SSTableMeta>> {
        const BATCH_SIZE: usize = 2;
        
        let mut outputs = Vec::new();
        
        for (batch_idx, chunk) in sources.chunks(BATCH_SIZE).enumerate() {
            let output_meta = self.merge_sstables_incremental(sublevel, batch_idx, chunk)?;
            outputs.push(output_meta);
        }
        
        Ok(outputs)
    }
    
    /// Merge a small batch of SSTables (for incremental compaction)
    #[allow(dead_code)]
    fn merge_sstables_incremental(
        &self,
        sublevel: usize,
        batch_idx: usize,
        sources: &[SSTableMeta],
    ) -> Result<SSTableMeta> {
        // Open all input SSTables
        let mut all_inputs = Vec::new();
        for meta in sources.iter() {
            match SSTable::open(&meta.path) {
                Ok(sstable) => all_inputs.push(sstable),
                Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    debug_log!("⚠️ SSTable disappeared during open: {:?}", meta.path);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        if all_inputs.is_empty() {
            return Err(StorageError::Index(
                "All input SSTables disappeared during incremental merge".into()
            ));
        }
        
        // Generate output file path
        let stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        let output_id = stats.num_compactions;
        let output_path = self.storage_dir.join(format!("l0_{}_batch{:02}_{:06}.sst", sublevel, batch_idx, output_id));
        drop(stats);
        
        // Streaming merge (same as full merge)
        let mut iters: Vec<_> = all_inputs.into_iter()
            .filter_map(|mut sst| sst.iter().ok())
            .collect();
        
        if iters.is_empty() {
            return Err(StorageError::Index("No valid iterators for incremental merge".into()));
        }
        
        let estimated_size = sources.len() * 1000;
        let mut builder = SSTableBuilder::new(&output_path, self.config.lsm_config.clone(), estimated_size)?;
        
        // Multi-way merge-sort (same algorithm as full merge)
        use std::collections::BinaryHeap;
        
        #[derive(Debug, Clone)]
        struct MergeEntry {
            key: u64,
            value: super::Value,
            iter_idx: usize,
        }
        
        impl Eq for MergeEntry {}
        impl PartialEq for MergeEntry {
            fn eq(&self, other: &Self) -> bool {
                self.key == other.key && self.iter_idx == other.iter_idx
            }
        }
        
        impl Ord for MergeEntry {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                other.key.cmp(&self.key)
            }
        }
        
        impl PartialOrd for MergeEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(MergeEntry { key, value, iter_idx: idx });
            }
        }
        
        let now_micros = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let tombstone_ttl_micros: u64 = 86_400 * 1_000_000; // 24 hours in microseconds

        let mut last_key: Option<u64> = None;
        let mut last_value: Option<super::Value> = None;

        while let Some(entry) = heap.pop() {
            if Some(entry.key) == last_key {
                if let Some(ref mut last) = last_value {
                    if entry.value.timestamp > last.timestamp {
                        *last = entry.value;
                    }
                }
            } else {
                if let (Some(key), Some(value)) = (last_key, last_value.take()) {
                    if !value.deleted || (now_micros.saturating_sub(value.timestamp) < tombstone_ttl_micros) {
                        builder.add(key, value)?;
                    }
                }

                last_key = Some(entry.key);
                last_value = Some(entry.value);
            }

            if let Some((key, value)) = iters[entry.iter_idx].next() {
                heap.push(MergeEntry { key, value, iter_idx: entry.iter_idx });
            }
        }

        if let (Some(key), Some(value)) = (last_key, last_value) {
            if !value.deleted || (now_micros.saturating_sub(value.timestamp) < tombstone_ttl_micros) {
                builder.add(key, value)?;
            }
        }
        
        let output_meta = builder.finish()?;
        
        // Update write stats
        let mut stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        stats.bytes_written += output_meta.size;
        if stats.bytes_read > 0 {
            stats.write_amplification = stats.bytes_written as f64 / stats.bytes_read as f64;
        }
        
        Ok(output_meta)
    }
}
