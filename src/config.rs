//! Database configuration and durability levels
//!
//! Provides flexible configuration options for balancing performance and safety.

use serde::{Deserialize, Serialize};

/// 持久性级别（Durability Level）
/// 
/// 在数据安全性和写入性能之间做权衡：
/// - Synchronous: 最安全，每次写入立即 fsync
/// - GroupCommit: 平衡性能和安全，多个请求共享 fsync
/// - Periodic: 高性能，定期批量 fsync
/// - NoSync: 最快但不安全，仅用于测试
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DurabilityLevel {
    /// 同步模式：每次写入立即 fsync（最安全，最慢）
    /// 
    /// 性能：~50 ops/s
    /// 延迟：~20ms
    /// 安全性：100% 安全，崩溃后零数据丢失
    /// 适用场景：金融交易、支付系统、关键业务数据
    /// 
    /// 等价于：
    /// - MySQL: innodb_flush_log_at_trx_commit = 1
    /// - PostgreSQL: synchronous_commit = on
    Synchronous,
    
    /// Group Commit：多个并发事务共享一次 fsync（推荐）
    /// 
    /// 性能：5K-10K ops/s（8-16线程并发）
    /// 延迟：1-5ms
    /// 安全性：100% 安全
    /// 适用场景：**大多数生产环境**
    /// 
    /// 工作原理：
    /// - 多个线程同时提交时，第一个线程负责批量刷盘
    /// - 其他线程等待刷盘完成
    /// - 所有线程共享一次 fsync 的成本
    /// 
    /// 配置参数：
    /// - max_batch_size: 单次刷盘的最大记录数
    /// - max_wait_us: 最大等待时间（微秒）
    GroupCommit {
        /// 单次批量刷盘的最大记录数（默认：1000）
        max_batch_size: usize,
        
        /// 最大等待时间（微秒），超时后强制刷盘（默认：1000 = 1ms）
        max_wait_us: u64,
    },
    
    /// 定期刷盘：后台线程定期 fsync（高性能，有数据丢失风险）
    /// 
    /// 性能：50K+ ops/s
    /// 延迟：<1ms
    /// 安全性：崩溃时可能丢失最近 N 毫秒的数据
    /// 适用场景：日志收集、监控数据、可容忍少量丢失的场景
    /// 
    /// 等价于：
    /// - MySQL: innodb_flush_log_at_trx_commit = 2
    /// - PostgreSQL: synchronous_commit = off
    /// 
    /// 配置参数：
    /// - interval_ms: 刷盘间隔（毫秒）
    Periodic {
        /// 刷盘间隔（毫秒），默认 100ms
        /// 
        /// 注意：崩溃时最多丢失该时间段内的数据
        /// - 100ms: 高性能，可接受少量丢失
        /// - 1000ms: 最高性能，但可能丢失1秒数据
        interval_ms: u64,
    },
    
    /// 不刷盘：只写入 OS 缓冲区（仅用于测试和基准测试）
    /// 
    /// 性能：100K+ ops/s
    /// 延迟：<0.1ms
    /// 安全性：⚠️ 非常不安全，崩溃时会丢失所有未刷盘数据
    /// 适用场景：**仅用于性能测试、开发调试**
    /// 
    /// ⚠️ 警告：生产环境禁止使用此模式！
    NoSync,
}

impl Default for DurabilityLevel {
    fn default() -> Self {
        // 默认使用 Group Commit（平衡性能和安全性）
        DurabilityLevel::GroupCommit {
            max_batch_size: 1000,
            max_wait_us: 1000, // 1ms
        }
    }
}

impl DurabilityLevel {
    /// 创建同步模式配置
    pub fn synchronous() -> Self {
        Self::Synchronous
    }
    
    /// 创建 Group Commit 配置（推荐）
    pub fn group_commit() -> Self {
        Self::GroupCommit {
            max_batch_size: 1000,
            max_wait_us: 1000,
        }
    }
    
    /// 创建自定义 Group Commit 配置
    pub fn group_commit_custom(max_batch_size: usize, max_wait_us: u64) -> Self {
        Self::GroupCommit {
            max_batch_size,
            max_wait_us,
        }
    }
    
    /// 创建定期刷盘配置
    pub fn periodic(interval_ms: u64) -> Self {
        Self::Periodic { interval_ms }
    }
    
    /// 创建不刷盘配置（仅测试用）
    pub fn no_sync() -> Self {
        Self::NoSync
    }
    
    /// 判断是否需要立即刷盘
    pub fn requires_immediate_sync(&self) -> bool {
        matches!(self, Self::Synchronous)
    }
    
    /// 判断是否完全不刷盘
    pub fn is_no_sync(&self) -> bool {
        matches!(self, Self::NoSync)
    }
    
    /// 获取人类可读的描述
    pub fn description(&self) -> &'static str {
        match self {
            Self::Synchronous => "同步模式（最安全）",
            Self::GroupCommit { .. } => "Group Commit（推荐）",
            Self::Periodic { .. } => "定期刷盘（高性能）",
            Self::NoSync => "不刷盘（仅测试）",
        }
    }
    
    /// 获取预期性能范围
    pub fn expected_throughput(&self) -> &'static str {
        match self {
            Self::Synchronous => "50 ops/s",
            Self::GroupCommit { .. } => "5K-10K ops/s",
            Self::Periodic { .. } => "50K+ ops/s",
            Self::NoSync => "100K+ ops/s",
        }
    }
    
    /// 获取数据安全等级
    pub fn safety_level(&self) -> &'static str {
        match self {
            Self::Synchronous | Self::GroupCommit { .. } => "100% 安全",
            Self::Periodic { interval_ms } => {
                if *interval_ms <= 100 {
                    "丢失 <100ms 数据"
                } else {
                    "丢失数据风险较高"
                }
            }
            Self::NoSync => "⚠️ 不安全",
        }
    }
}

/// WAL 配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WALConfig {
    /// 持久性级别
    pub durability_level: DurabilityLevel,
    
    /// WAL 文件目录（相对于数据库目录）
    pub wal_dir: String,
    
    /// 单个 WAL 文件的最大大小（字节）
    pub max_wal_size: u64,
    
    /// 是否启用 WAL 压缩
    pub enable_compression: bool,
}

impl Default for WALConfig {
    fn default() -> Self {
        Self {
            durability_level: DurabilityLevel::default(),
            wal_dir: "wal".to_string(),
            max_wal_size: 64 * 1024 * 1024, // 64MB
            enable_compression: false,
        }
    }
}

impl WALConfig {
    /// 创建用于金融场景的配置（最安全）
    pub fn for_financial() -> Self {
        Self {
            durability_level: DurabilityLevel::Synchronous,
            ..Default::default()
        }
    }
    
    /// 创建通用场景配置（推荐）
    pub fn for_general() -> Self {
        Self {
            durability_level: DurabilityLevel::group_commit(),
            ..Default::default()
        }
    }
    
    /// 创建高性能场景配置（日志/监控）
    pub fn for_logging() -> Self {
        Self {
            durability_level: DurabilityLevel::periodic(100),
            ..Default::default()
        }
    }
    
    /// 创建测试用配置（最快）
    pub fn for_testing() -> Self {
        Self {
            durability_level: DurabilityLevel::NoSync,
            ..Default::default()
        }
    }
}

/// 索引更新策略（Index Update Strategy）
/// 
/// 🚀 Phase 3+: 支持增量索引更新（Hybrid Mode）
/// 
/// 在写入性能和查询实时性之间做权衡：
/// - BatchOnly: 只在 checkpoint 时批量构建（默认，最高性能）
/// - Hybrid: 重要索引实时更新 + 其他索引批量构建
/// - Realtime: 所有索引实时更新（最低性能，最好的查询实时性）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[derive(Default)]
pub enum IndexUpdateStrategy {
    /// 批量构建模式（默认）：所有索引只在 checkpoint 时批量构建
    /// 
    /// 性能：最高（写入吞吐 +1,412%）
    /// 查询实时性：延迟（直到下次 checkpoint）
    /// 适用场景：批量导入、ETL、日志收集、离线分析
    /// 
    /// 说明：
    /// - PRIMARY KEY 索引仍然实时更新（保证查询性能）
    /// - 其他所有索引（vector, spatial, text, secondary columns）延迟构建
    /// - 写入时只写 MemTable，索引构建在 checkpoint 时自动触发
    #[default]
    BatchOnly,
    
    /// 混合模式（推荐）：重要索引实时更新 + 其他索引批量构建
    /// 
    /// 性能：高（写入吞吐 +300~500%）
    /// 查询实时性：好（重要索引立即可用）
    /// 适用场景：**大多数生产环境**
    /// 
    /// 实时更新的索引：
    /// - PRIMARY KEY（必须）
    /// - UNIQUE 索引
    /// - 高频查询的二级索引
    /// 
    /// 批量构建的索引：
    /// - 向量索引（DiskANN）
    /// - 空间索引（i-Octree）
    /// - 全文索引（FTS）
    /// - 低频查询的索引
    /// 
    /// 配置参数：
    /// - realtime_index_types: 哪些索引类型实时更新
    Hybrid {
        /// 实时更新的索引类型（其他类型批量构建）
        realtime_index_types: Vec<RealtimeIndexType>,
    },
    
    /// 实时模式：所有索引实时更新
    /// 
    /// 性能：低（与 Phase 3 之前相同）
    /// 查询实时性：最好（所有索引立即可用）
    /// 适用场景：交易系统、实时分析、在线服务
    /// 
    /// 说明：
    /// - 所有索引在插入时立即更新
    /// - 写入吞吐较低，但查询无延迟
    /// - 内存占用较高
    Realtime,
}

/// 实时更新的索引类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RealtimeIndexType {
    /// PRIMARY KEY 索引（必须实时更新）
    PrimaryKey,
    /// UNIQUE 索引
    Unique,
    /// 二级索引（WHERE 过滤）
    SecondaryColumn,
    /// 向量索引
    Vector,
    /// 空间索引
    Spatial,
    /// 全文索引
    FullText,
}


impl IndexUpdateStrategy {
    /// 创建批量构建模式
    pub fn batch_only() -> Self {
        Self::BatchOnly
    }
    
    /// 创建混合模式（推荐）
    pub fn hybrid_default() -> Self {
        Self::Hybrid {
            realtime_index_types: vec![
                RealtimeIndexType::PrimaryKey,
                RealtimeIndexType::Unique,
                RealtimeIndexType::SecondaryColumn,
            ],
        }
    }
    
    /// 创建自定义混合模式
    pub fn hybrid_custom(realtime_index_types: Vec<RealtimeIndexType>) -> Self {
        Self::Hybrid { realtime_index_types }
    }
    
    /// 创建实时模式
    pub fn realtime() -> Self {
        Self::Realtime
    }
    
    /// 判断是否需要实时更新指定索引类型
    pub fn should_update_realtime(&self, index_type: RealtimeIndexType) -> bool {
        match self {
            Self::BatchOnly => {
                // 只有 PRIMARY KEY 实时更新
                index_type == RealtimeIndexType::PrimaryKey
            }
            Self::Hybrid { realtime_index_types } => {
                realtime_index_types.contains(&index_type)
            }
            Self::Realtime => true,  // 所有索引实时更新
        }
    }
    
    /// 获取人类可读的描述
    pub fn description(&self) -> &'static str {
        match self {
            Self::BatchOnly => "批量构建（最高性能）",
            Self::Hybrid { .. } => "混合模式（推荐）",
            Self::Realtime => "实时模式（最低延迟）",
        }
    }
}

/// 数据库配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DBConfig {
    /// WAL 配置
    pub wal_config: WALConfig,
    
    /// 分区数量
    pub num_partitions: u8,
    
    /// LSM 树配置
    pub lsm_config: LSMConfig,
    
    /// 是否启用统计信息
    pub enable_stats: bool,
    
    /// 🚀 P1: Row cache size (number of rows, None = use default 10000)
    ///
    /// Memory usage: ~1KB/row × cache_size
    /// - 10000 rows ≈ 10MB (default)
    /// - 50000 rows ≈ 50MB (high traffic)
    /// - 1000 rows ≈ 1MB (memory-constrained)
    pub row_cache_size: Option<usize>,

    /// PK lookup cache capacity (number of entries per table)
    ///
    /// This bounds the in-memory PK→RowId mapping. When exceeded, least-recently-used
    /// entries are evicted and queries fall back to the disk-based column index.
    ///
    /// Memory usage: ~80 bytes/entry × capacity
    /// - 50000 (default) ≈ 4MB per table
    /// - 10000 ≈ 800KB per table (memory-constrained)
    pub pk_lookup_capacity: usize,
    
    /// 🚀 Phase 3+: Index update strategy
    /// 
    /// Controls when indexes are updated:
    /// - BatchOnly: All indexes built during checkpoint (highest performance)
    /// - Hybrid: Important indexes realtime + others batch (recommended)
    /// - Realtime: All indexes updated immediately (lowest latency)
    pub index_update_strategy: IndexUpdateStrategy,
    
    /// 🚀 P0: Query timeout (seconds)
    /// 
    /// Maximum time allowed for a single query to execute.
    /// - None = No timeout (default, may cause long-running queries)
    /// - Some(30) = 30 seconds timeout (recommended for concurrent workloads)
    /// 
    /// When timeout is reached:
    /// - Query is aborted immediately
    /// - Returns StorageError::Timeout
    /// - Releases locks to prevent deadlocks
    pub query_timeout_secs: Option<u64>,
    
    /// 🚀 Auto-checkpoint configuration
    /// 
    /// Automatically triggers checkpoint to clean up WAL files when:
    /// - WAL size exceeds threshold, OR
    /// - Time interval reached
    /// 
    /// None = Disabled (user must manually call checkpoint())
    /// Some(...) = Enabled with automatic cleanup
    pub auto_checkpoint: Option<AutoCheckpointConfig>,

    /// Columnar store configuration (for TimeSeries tables)
    pub columnar_config: crate::storage::columnar::config::ColumnarConfig,
}

/// Auto-checkpoint trigger configuration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoCheckpointConfig {
    /// Maximum WAL size before auto-checkpoint (bytes)
    /// Default: 16MB (prevents unlimited WAL growth)
    pub max_wal_size_bytes: u64,
    
    /// Minimum time interval between checkpoints (seconds)
    /// Default: 60 seconds (prevents too-frequent checkpoints)
    pub min_interval_secs: u64,
}

impl Default for AutoCheckpointConfig {
    fn default() -> Self {
        Self {
            max_wal_size_bytes: 16 * 1024 * 1024,  // 16MB
            min_interval_secs: 60,  // 1 minute
        }
    }
}

impl AutoCheckpointConfig {
    /// Aggressive cleanup: small WAL size, frequent checkpoints
    /// 
    /// Use case: Low-memory environments, frequent writes
    /// CPU: ~0.2% overhead, Memory: < 1MB
    pub fn aggressive() -> Self {
        Self {
            max_wal_size_bytes: 4 * 1024 * 1024,  // 4MB
            min_interval_secs: 30,  // 30 seconds (increased from 10s to reduce CPU)
        }
    }
    
    /// Relaxed cleanup: larger WAL size, less frequent checkpoints
    /// 
    /// Use case: High-memory environments, batch workloads
    /// CPU: < 0.05% overhead, Memory: up to 64MB WAL
    pub fn relaxed() -> Self {
        Self {
            max_wal_size_bytes: 64 * 1024 * 1024,  // 64MB
            min_interval_secs: 300,  // 5 minutes
        }
    }
    
    /// Embedded/IoT optimized: minimal resource usage
    /// 
    /// Use case: Embedded devices, IoT, mobile apps
    /// - Very small WAL size (2MB)
    /// - Infrequent checks (120s = 2 minutes)
    /// - Minimal CPU overhead (< 0.01%)
    /// - Low memory footprint
    pub fn embedded() -> Self {
        Self {
            max_wal_size_bytes: 2 * 1024 * 1024,  // 2MB (tight limit)
            min_interval_secs: 120,  // 2 minutes (fewer wakeups)
        }
    }
}

impl Default for DBConfig {
    fn default() -> Self {
        Self {
            wal_config: WALConfig::default(),
            num_partitions: 4,
            lsm_config: LSMConfig::default(),
            enable_stats: true,
            row_cache_size: None,  // Use default 10000
            pk_lookup_capacity: 50_000,  // ~4MB per table
            index_update_strategy: IndexUpdateStrategy::default(),  // BatchOnly
            query_timeout_secs: None,  // No timeout by default
            auto_checkpoint: Some(AutoCheckpointConfig::default()),  // ✅ 默认启用自动 checkpoint
            columnar_config: crate::storage::columnar::config::ColumnarConfig::default(),
        }
    }
}

impl DBConfig {
    /// 创建用于金融场景的配置
    pub fn for_financial() -> Self {
        Self {
            wal_config: WALConfig::for_financial(),
            ..Default::default()
        }
    }
    
    /// 创建通用场景配置
    pub fn for_general() -> Self {
        Self {
            wal_config: WALConfig::for_general(),
            ..Default::default()
        }
    }
    
    /// 创建高性能场景配置
    pub fn for_high_performance() -> Self {
        Self {
            wal_config: WALConfig::for_logging(),
            ..Default::default()
        }
    }
    
    /// 创建测试用配置
    pub fn for_testing() -> Self {
        Self {
            wal_config: WALConfig::for_testing(),
            ..Default::default()
        }
    }

    /// Edge/IoT optimized configuration for resource-constrained devices
    ///
    /// Use case: Embedded devices, IoT sensors, AR glasses, robots
    /// - WAL: Periodic (50ms), 8MB max - tolerates small data loss window
    /// - LSM: 4MB memtable - low memory footprint
    /// - num_partitions: 2 - fewer threads, less overhead
    /// - row_cache_size: 500 - minimal cache (~500KB)
    /// - auto_checkpoint: embedded() - infrequent wakeups
    /// - index_update_strategy: BatchOnly - highest write throughput
    pub fn for_edge() -> Self {
        Self {
            wal_config: WALConfig {
                durability_level: DurabilityLevel::Periodic { interval_ms: 50 },
                max_wal_size: 8 * 1024 * 1024, // 8MB
                ..Default::default()
            },
            num_partitions: 2,
            lsm_config: LSMConfig {
                memtable_size_limit: 4 * 1024 * 1024, // 4MB
                ..Default::default()
            },
            row_cache_size: Some(500),
            pk_lookup_capacity: 10_000,  // ~800KB per table (memory-constrained)
            auto_checkpoint: Some(AutoCheckpointConfig::embedded()),
            index_update_strategy: IndexUpdateStrategy::BatchOnly,
            ..Default::default()
        }
    }

    /// Robotics preset: optimized for high-frequency sensor ingestion.
    ///
    /// Targets: IMU 100Hz-1kHz, motor controllers, occasional image embeddings.
    /// Memory: ~20MB typical, ~50MB peak.
    ///
    /// - WAL: Periodic 50ms, 32MB (tolerates small data loss window)
    /// - LSM: 4MB memtable, 2 partitions (low memory, fewer threads)
    /// - Auto-checkpoint: 8MB WAL trigger, 60s interval
    /// - Index strategy: BatchOnly (highest throughput for sensor data)
    pub fn for_robotics() -> Self {
        Self {
            wal_config: WALConfig {
                durability_level: DurabilityLevel::Periodic { interval_ms: 50 },
                max_wal_size: 32 * 1024 * 1024, // 32MB
                ..Default::default()
            },
            num_partitions: 2,
            lsm_config: LSMConfig {
                memtable_size_limit: 4 * 1024 * 1024, // 4MB
                ..Default::default()
            },
            row_cache_size: Some(500),
            pk_lookup_capacity: 10_000,
            auto_checkpoint: Some(AutoCheckpointConfig {
                max_wal_size_bytes: 8 * 1024 * 1024, // 8MB
                min_interval_secs: 60,
            }),
            index_update_strategy: IndexUpdateStrategy::BatchOnly,
            ..Default::default()
        }
    }
}

/// LSM 树配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LSMConfig {
    /// MemTable 大小限制（字节）
    pub memtable_size_limit: usize,
    
    /// Level 0 SSTable 数量阈值（触发合并）
    pub level0_compaction_threshold: usize,
    
    /// 布隆过滤器假阳性率
    pub bloom_filter_false_positive_rate: f64,
}

impl Default for LSMConfig {
    fn default() -> Self {
        Self {
            memtable_size_limit: 4 * 1024 * 1024, // 4MB (stable memory for embedded use)
            level0_compaction_threshold: 4,
            bloom_filter_false_positive_rate: 0.01, // 1%
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_durability_levels() {
        let sync = DurabilityLevel::Synchronous;
        assert!(sync.requires_immediate_sync());
        assert_eq!(sync.expected_throughput(), "50 ops/s");
        
        let group = DurabilityLevel::group_commit();
        assert!(!group.requires_immediate_sync());
        assert_eq!(group.safety_level(), "100% 安全");
        
        let no_sync = DurabilityLevel::NoSync;
        assert!(no_sync.is_no_sync());
        assert!(no_sync.safety_level().contains("不安全"));
    }
    
    #[test]
    fn test_config_presets() {
        let financial = DBConfig::for_financial();
        assert!(financial.wal_config.durability_level.requires_immediate_sync());
        
        let general = DBConfig::for_general();
        assert!(!general.wal_config.durability_level.requires_immediate_sync());
        
        let testing = DBConfig::for_testing();
        assert!(testing.wal_config.durability_level.is_no_sync());
    }
}
