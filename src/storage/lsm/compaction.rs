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
use crate::{Result, StorageError};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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
            // Tiered compaction: select from sublevel
            if let Some(sublevel_idx) = self.get_sublevel_to_compact() {
                if let Some(ref sublevels) = self.sublevels {
                    if sublevel_idx < sublevels.len() {
                        return sublevels[sublevel_idx].sstables.clone();
                    }
                }
            }
            
            // Fallback: compact all overlapping files (legacy)
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
}

impl CompactionWorker {
    /// Create a new compaction worker
    pub fn new(storage_dir: PathBuf, config: &LSMConfig) -> Self {
        let mut levels = Vec::new();
        for level in 0..config.num_levels {
            levels.push(Level::new(level, config));
        }
        
        Self {
            storage_dir,
            levels: Arc::new(Mutex::new(levels)),
            config: CompactionConfig {
                lsm_config: config.clone(),
            },
            stats: Arc::new(Mutex::new(CompactionStats::default())),
        }
    }
    
    /// Register a new SSTable (from MemTable flush)
    pub fn register_sstable(&self, meta: SSTableMeta) -> Result<()> {
        let mut levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        // Always add to L0
        levels[0].add_sstable(meta);
        
        Ok(())
    }
    
    /// Check if compaction is needed
    pub fn needs_compaction(&self) -> Result<bool> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        Ok(levels.iter().any(|level| level.needs_compaction()))
    }
    
    /// Get all SSTables across all levels (for query)
    pub fn get_all_sstables(&self) -> Result<Vec<SSTableMeta>> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        let mut all_sstables = Vec::new();
        for level in levels.iter() {
            all_sstables.extend(level.sstables.iter().cloned());
        }
        
        Ok(all_sstables)
    }
    
    /// Run one round of compaction
    pub fn run_compaction(&self) -> Result<()> {
        let levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        // Find first level that needs compaction
        let level_idx = match levels.iter().position(|l| l.needs_compaction()) {
            Some(idx) => idx,
            None => return Ok(()), // No compaction needed
        };
        
        // ✨ Special handling for L0 tiered compaction
        if level_idx == 0 {
            if let Some(sublevel_idx) = levels[0].get_sublevel_to_compact() {
                drop(levels);  // Release lock before I/O
                return self.run_tiered_compaction(sublevel_idx);
            }
        }
        
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
        let mut levels = self.levels.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        
        // Remove source files (only those that actually existed)
        for source in &valid_sources {
            levels[level_idx].remove_sstable(&source.path);
            let _ = fs::remove_file(&source.path);
        }
        
        // Also clean up metadata for files that didn't exist
        for source in &sources {
            if !valid_sources.iter().any(|v| v.path == source.path) {
                levels[level_idx].remove_sstable(&source.path);
            }
        }
        
        // Remove overlapping files (only those that actually existed)
        for overlap in &valid_overlapping {
            levels[level_idx + 1].remove_sstable(&overlap.path);
            let _ = fs::remove_file(&overlap.path);
        }
        
        // Also clean up metadata for files that didn't exist
        for overlap in &overlapping {
            if !valid_overlapping.iter().any(|v| v.path == overlap.path) {
                levels[level_idx + 1].remove_sstable(&overlap.path);
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
        // Open all input SSTables
        let mut all_inputs = Vec::new();
        for meta in sources.iter().chain(overlapping.iter()) {
            match SSTable::open(&meta.path) {
                Ok(sstable) => all_inputs.push(sstable),
                Err(StorageError::Io(ref e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    // File disappeared between filter and open (rare race condition)
                    eprintln!("⚠️ SSTable disappeared during open: {:?}", meta.path);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        
        if all_inputs.is_empty() {
            // All files disappeared - should not happen since we pre-filtered
            return Err(StorageError::Index(
                "All input SSTables disappeared during compaction".into()
            ));
        }
        
        // Generate output file path
        let stats = self.stats.lock()
            .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
        let output_id = stats.num_compactions;
        let output_path = self.storage_dir.join(format!("l{}_{:06}.sst", output_level, output_id));
        drop(stats); // 🔧 Release lock early
        
        // 🔧 Streaming merge: Use iterator merge instead of BTreeMap
        // This avoids loading all data into memory at once
        
        // Step 1: Collect all iterators
        let mut iters: Vec<_> = all_inputs.into_iter()
            .filter_map(|mut sst| sst.iter().ok())
            .collect();
        
        if iters.is_empty() {
            return Err(StorageError::Index("No valid iterators for compaction".into()));
        }
        
        // Step 2: Use a conservative estimate (we don't know exact count without iterating)
        let estimated_size = sources.len() + overlapping.len() * 1000; // Conservative guess
        
        // Build output SSTable using streaming merge
        let mut builder = SSTableBuilder::new(&output_path, self.config.lsm_config.clone(), estimated_size)?;
        
        // 🔧 Multi-way merge-sort with priority queue
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
                // Min-heap: smallest key first
                other.key.cmp(&self.key)
            }
        }
        
        impl PartialOrd for MergeEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        
        // Initialize heap with first entry from each iterator
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some((key, value)) = iter.next() {
                heap.push(MergeEntry {
                    key,
                    value,
                    iter_idx: idx,
                });
            }
        }
        
        // Tombstone TTL
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let tombstone_ttl = 86400; // 24 hours
        
        // Merge-sort with deduplication
        let mut last_key: Option<u64> = None;
        let mut last_value: Option<super::Value> = None;
        
        while let Some(entry) = heap.pop() {
            // Check if this is a duplicate key
            if Some(entry.key) == last_key {
                // Keep entry with highest timestamp
                if let Some(ref mut last) = last_value {
                    if entry.value.timestamp > last.timestamp {
                        *last = entry.value;
                    }
                }
            } else {
                // Write previous key (if exists)
                if let (Some(key), Some(value)) = (last_key, last_value.take()) {
                    // Skip old tombstones
                    if !value.deleted || (now - value.timestamp < tombstone_ttl) {
                        builder.add(key, value)?;
                    }
                }
                
                // Start tracking new key
                last_key = Some(entry.key);
                last_value = Some(entry.value);
            }
            
            // Fetch next entry from this iterator
            if let Some((key, value)) = iters[entry.iter_idx].next() {
                heap.push(MergeEntry {
                    key,
                    value,
                    iter_idx: entry.iter_idx,
                });
            }
        }
        
        // Write final key
        if let (Some(key), Some(value)) = (last_key, last_value) {
            if !value.deleted || (now - value.timestamp < tombstone_ttl) {
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
            
            // Update levels
            let mut levels = self.levels.lock()
                .map_err(|_| StorageError::Lock("Lock poisoned".into()))?;
            
            // Remove from L0 sublevel
            if let Some(ref mut sublevels) = levels[0].sublevels {
                if sublevel_idx < sublevels.len() {
                    sublevels[sublevel_idx].sstables.clear();
                }
            }
            
            // Remove from L0 main list
            for source in &valid_sources {
                levels[0].remove_sstable(&source.path);
                let _ = std::fs::remove_file(&source.path);
            }
            
            // Remove overlapping from L1
            for overlap in &valid_overlapping {
                levels[1].remove_sstable(&overlap.path);
                let _ = std::fs::remove_file(&overlap.path);
            }
            
            // Add to L1
            levels[1].add_sstable(output_meta);
        } else {
            // ✨ Incremental merge to next sublevel (P2 Phase 3.2)
            let output_metas = self.incremental_merge(&valid_sources, sublevel_idx)?;
            
            // Update sublevels
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
            
            // Update main list (separate from sublevels borrow)
            for meta in output_metas {
                levels[0].sstables.push(meta);
            }
            
            // Remove source files
            for source in &valid_sources {
                let _ = std::fs::remove_file(&source.path);
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
        
        Ok(())
    }
    
    /// ✨ P2 Phase 3.2: Incremental merge (batch size = 2)
    /// 
    /// Instead of merging all N files at once:
    /// - Split into batches of 2
    /// - Merge each batch independently
    /// - Reduces single-merge data volume by 50%
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
                    eprintln!("⚠️ SSTable disappeared during open: {:?}", meta.path);
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
        
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let tombstone_ttl = 86400;
        
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
                    if !value.deleted || (now - value.timestamp < tombstone_ttl) {
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
            if !value.deleted || (now - value.timestamp < tombstone_ttl) {
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
