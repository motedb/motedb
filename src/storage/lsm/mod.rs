//! LSM-Tree Storage Engine
//!
//! ## Architecture
//! - **MemTable**: In-memory skip list (write buffer)
//! - **SSTable**: Sorted String Table (disk persistence)
//! - **Compaction**: Background merge/compress
//!
//! ## Performance Targets
//! - Write: 10K+ ops/s
//! - Read: < 1ms (P99)
//! - Space: 2:1 compression ratio

mod memtable;
mod unified_memtable;  // 🆕 Unified MemTable (数据 + 向量)
mod sstable;
mod compaction;
mod engine;
mod bloom;
mod blobstore;
mod merging_iterator;  // 🚀 流式合并迭代器

pub use memtable::MemTable;
pub use unified_memtable::{UnifiedMemTable, UnifiedEntry};  // 🆕 Export
pub use sstable::{SSTable, SSTableBuilder, BlockIndex};
pub use compaction::{CompactionWorker, CompactionConfig, Level, SSTableMeta, CompactionStats};
pub use engine::{LSMEngine, LSMBatchedIterator};  // 🚀 Export batched iterator
pub use bloom::BloomFilter;
pub use blobstore::BlobStore;
pub use merging_iterator::MergingIterator;  // 🚀 Export merging iterator

/// Key type (row_id as u64)
/// 
/// 🔧 优化：从 Vec<u8> 改为 u64
/// - 消除 24 bytes Vec 元数据开销 (↓ 75%)
/// - 零拷贝，无堆分配
/// - BTreeMap<u64> 比 BTreeMap<Vec<u8>> 快 2-3x
pub type Key = u64;

/// Blob reference for large values
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobRef {
    /// Blob file ID
    pub file_id: u32,
    /// Offset in blob file
    pub offset: u64,
    /// Size of blob data
    pub size: u32,
}

/// Value storage type
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ValueData {
    /// Inline small value (< blob_threshold)
    Inline(Vec<u8>),
    /// Reference to blob file for large value
    Blob(BlobRef),
}

impl ValueData {
    /// Get size of value data (for inline: actual data size, for blob: ref size)
    pub fn len(&self) -> usize {
        match self {
            ValueData::Inline(data) => data.len(),
            ValueData::Blob(_) => 16,  // BlobRef is 16 bytes (file_id + offset + size)
        }
    }
    
    pub fn is_empty(&self) -> bool {
        match self {
            ValueData::Inline(data) => data.is_empty(),
            ValueData::Blob(_) => false,
        }
    }
}

/// Value with MVCC metadata
#[derive(Clone, Debug)]
pub struct Value {
    /// Data payload (inline or blob reference)
    pub data: ValueData,
    
    /// MVCC timestamp (transaction ID)
    pub timestamp: u64,
    
    /// Tombstone marker (for deletion)
    pub deleted: bool,
}

impl Value {
    pub fn new(data: Vec<u8>, timestamp: u64) -> Self {
        Self {
            data: ValueData::Inline(data),
            timestamp,
            deleted: false,
        }
    }
    
    pub fn new_blob(blob_ref: BlobRef, timestamp: u64) -> Self {
        Self {
            data: ValueData::Blob(blob_ref),
            timestamp,
            deleted: false,
        }
    }
    
    pub fn tombstone(timestamp: u64) -> Self {
        Self {
            data: ValueData::Inline(Vec::new()),
            timestamp,
            deleted: true,
        }
    }
    
    /// Get inline data if available
    pub fn as_inline(&self) -> Option<&[u8]> {
        match &self.data {
            ValueData::Inline(data) => Some(data),
            ValueData::Blob(_) => None,
        }
    }
    
    /// Check if this is a blob reference
    pub fn is_blob(&self) -> bool {
        matches!(self.data, ValueData::Blob(_))
    }
}

/// LSM-Tree configuration
#[derive(Clone, Debug)]
pub struct LSMConfig {
    /// MemTable size threshold (default 4MB)
    pub memtable_size: usize,
    
    /// SSTable block size (default 64KB)
    pub block_size: usize,
    
    /// Number of levels (default 7)
    pub num_levels: usize,
    
    /// Level size multiplier (default 10)
    pub level_multiplier: usize,
    
    /// L0 compaction trigger (default 4 files)
    pub l0_compaction_trigger: usize,
    
    /// Bloom filter bits per key (default 10)
    pub bloom_bits_per_key: usize,
    
    /// Enable compression (default true)
    pub enable_compression: bool,
    
    /// Blob threshold: values larger than this go to blob files (default 32KB)
    pub blob_threshold: usize,
    
    /// Blob file size limit (default 256MB)
    pub blob_file_size: usize,
    
    /// 🆕 SSTable cache size (number of cached SSTable handles, default 8)
    pub sstable_cache_size: usize,
    
    /// 🚀 P0: Memory limit for SSTable cache (bytes)
    /// 
    /// When total cache size exceeds this limit, LRU eviction is triggered.
    /// - None = No limit (uses sstable_cache_size only)
    /// - Some(200MB) = Max 200MB cache memory (recommended for production)
    /// 
    /// Calculation: cache_size × avg_sstable_size
    /// - 8 SSTables × 25MB = 200MB (default)
    /// - For embedded: 4 SSTables × 10MB = 40MB
    pub sstable_cache_memory_limit_mb: Option<usize>,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size: 512 * 1024,          // 🚀 P0: 512KB - 激进控制内存（原4MB过大）
            block_size: 64 * 1024,              // 64KB (optimal for compression)
            num_levels: 7,
            level_multiplier: 10,
            l0_compaction_trigger: 2,           // 🔧 2个文件就触发compaction，减少L0积压
            bloom_bits_per_key: 12,             // 12 bits - 降低false positive率
            enable_compression: true,
            blob_threshold: 32 * 1024,          // 32KB (separate large values/vectors)
            blob_file_size: 256 * 1024 * 1024,  // 256MB per blob file
            sstable_cache_size: 8,              // 🔧 8个SSTable缓存（减少内存）
            sstable_cache_memory_limit_mb: Some(200),  // 🚀 P0: 200MB memory limit
        }
    }
}

impl LSMConfig {
    /// Optimized config for read-heavy workloads
    pub fn read_optimized() -> Self {
        Self {
            memtable_size: 4 * 1024 * 1024,
            block_size: 32 * 1024,              // Smaller blocks for faster seeks
            num_levels: 7,
            level_multiplier: 10,
            l0_compaction_trigger: 2,           // Aggressive compaction
            bloom_bits_per_key: 16,             // More accurate bloom filters
            enable_compression: true,
            blob_threshold: 32 * 1024,
            blob_file_size: 256 * 1024 * 1024,
            sstable_cache_size: 16,             // More cache for reads
            sstable_cache_memory_limit_mb: Some(400),  // 🚀 P0: 400MB for read-heavy
        }
    }
    
    /// Optimized config for write-heavy workloads
    pub fn write_optimized() -> Self {
        Self {
            memtable_size: 16 * 1024 * 1024,    // Larger buffer
            block_size: 128 * 1024,             // Larger blocks for batch writes
            num_levels: 6,                       // Fewer levels
            level_multiplier: 8,                 // Lower multiplier
            l0_compaction_trigger: 8,           // Lazy compaction
            bloom_bits_per_key: 8,              // Smaller bloom filters
            enable_compression: true,
            blob_threshold: 32 * 1024,
            blob_file_size: 256 * 1024 * 1024,
            sstable_cache_size: 8,
            sstable_cache_memory_limit_mb: Some(200),  // 🚀 P0
        }
    }
    
    /// 🆕 Embedded Mode: 嵌入式设备优化配置
    /// 
    /// **目标**: 减少 50% 内存占用，适用于嵌入式数据库
    /// 
    /// **优化策略**:
    /// 1. ✅ 更小的 MemTable（2MB vs 4MB）→ 峰值内存 -50%
    /// 2. ✅ 更小的 Block（32KB vs 64KB）→ SSTable -50%
    /// 3. ✅ 强制压缩（Snappy）→ 磁盘占用 -40%
    /// 4. ✅ 激进的 L0 压缩（触发阈值=2）→ 快速合并小文件
    /// 5. ✅ 更小的 Bloom Filter（8 bits）→ 减少元数据开销
    /// 6. ✅ 更小的 Blob 阈值（16KB vs 32KB）→ 更多数据进入 Blob
    /// 7. ✅ 更小的缓存（4个 vs 8个）→ 缓存内存 -50%
    /// 8. ✅ 6 层 LSM（vs 7 层）→ 减少空间放大
    /// 
    /// **适用场景**:
    /// - 嵌入式数据库（Electron, Mobile, IoT）
    /// - 内存受限环境（< 512MB RAM）
    /// - 单机应用（无需高并发）
    /// 
    /// **性能权衡**:
    /// - 写入吞吐：-20%（更小的 buffer）
    /// - 读取延迟：+10%（更小的 cache）
    /// - 内存占用：**-50%** ✅
    pub fn embedded() -> Self {
        Self {
            memtable_size: 2 * 1024 * 1024,         // 2MB
            block_size: 32 * 1024,                   // 32KB
            num_levels: 6,                           // 6 层
            level_multiplier: 8,                     // 8x
            l0_compaction_trigger: 2,
            bloom_bits_per_key: 8,                   // 8 bits
            enable_compression: true,
            blob_threshold: 16 * 1024,               // 16KB
            blob_file_size: 128 * 1024 * 1024,       // 128MB
            sstable_cache_size: 4,                   // 4 个
            sstable_cache_memory_limit_mb: Some(40),  // 🚀 P0: 40MB for embedded
        }
    }
    
    /// 🆕 Tiny Mode: 微型设备优化配置（IoT / 移动端）
    /// 
    /// **目标**: 减少 70% 内存占用，总内存 < 20MB
    /// 
    /// **适用场景**:
    /// - IoT 设备（< 128MB RAM）
    /// - 移动应用（省电模式）
    /// - 边缘计算设备
    /// 
    /// **性能权衡**:
    /// - 写入吞吐：-40%
    /// - 读取延迟：+20%
    /// - 内存占用：**-70%** ✅
    pub fn tiny() -> Self {
        Self {
            memtable_size: 1024 * 1024,         // 1MB
            block_size: 16 * 1024,                   // 16KB
            num_levels: 5,                           // 5 层
            level_multiplier: 4,                     // 4x
            l0_compaction_trigger: 2,
            bloom_bits_per_key: 6,                   // 6 bits
            enable_compression: true,
            blob_threshold: 8 * 1024,                // 8KB
            blob_file_size: 64 * 1024 * 1024,        // 64MB
            sstable_cache_size: 2,                   // 2 个
            sstable_cache_memory_limit_mb: Some(20),  // 🚀 P0: 20MB for tiny
        }
    }
    
    /// 🆕 P1 Memory Optimized Config (Low Memory Footprint)
    /// 
    /// **Target**: 减少30-50%内存占用
    /// 
    /// **适用场景**:
    /// - 内存受限环境（< 512MB）
    /// - 大数据量场景（> 100万条记录）
    /// - 磁盘空间充足但内存紧张
    /// 
    /// **性能权衡**:
    /// - 写入延迟: +10-20%（更频繁的flush）
    /// - 查询延迟: +5-10%（更多SSTable文件）
    /// - 内存占用: -30-50% ✅
    /// - 磁盘占用: -30-50% ✅（压缩）
    pub fn memory_optimized() -> Self {
        Self {
            memtable_size: 2 * 1024 * 1024,     // 🔧 2MB（减少峰值内存）
            block_size: 32 * 1024,              // 🔧 32KB（减少SSTable大小）
            num_levels: 7,
            level_multiplier: 10,
            l0_compaction_trigger: 2,           // 🔧 激进压缩（快速合并）
            bloom_bits_per_key: 10,             // 🔧 10 bits（减少元数据）
            enable_compression: true,           // ✅ 强制启用Snappy压缩
            blob_threshold: 16 * 1024,          // 🔧 16KB（更多数据进Blob）
            blob_file_size: 128 * 1024 * 1024,  // 🔧 128MB（减少Blob文件大小）
            sstable_cache_size: 4,              // 🔧 4个缓存（最小化内存）
            sstable_cache_memory_limit_mb: Some(100),  // 🚀 P0: 100MB for memory-optimized
        }
    }
    
    /// Balanced config (current default)
    pub fn balanced() -> Self {
        Self::default()
    }
}
