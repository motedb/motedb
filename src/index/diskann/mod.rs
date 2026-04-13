//! FreshDiskANN - 融合 LSM 架构的向量索引
//! 
//! ## 架构概览
//! 
//! ```text
//! ┌─────────────────────────────────────┐
//! │  Level 0: Fresh Graph (内存)       │  ← 新插入数据
//! │  - 5000-10000 向量                  │
//! │  - 轻量级 Vamana 图                 │
//! │  - 原始 f32 向量                    │
//! └─────────────────────────────────────┘
//!            ↓ Flush
//! ┌─────────────────────────────────────┐
//! │  Level 1: SST Files (磁盘)         │  ← 不可变图文件
//! │  - 多个 .vamana.sst                 │
//! │  - SQ8 压缩向量                     │
//! │  - 冻结的图结构                     │
//! └─────────────────────────────────────┘
//!            ↓ Compaction
//! ┌─────────────────────────────────────┐
//! │  Level 2+: Merged Index (磁盘)     │  ← 全局优化图
//! │  - 单个 .vamana.merged              │
//! │  - SQ8 + PQ 压缩                    │
//! │  - 高质量 Vamana 图                 │
//! └─────────────────────────────────────┘
//! ```text
//! 
//! ## 核心组件
//! 
//! - `fresh_graph`: 内存层 Fresh Vamana 图
//! - `sst`: 磁盘层 SST 文件格式
//! - `multi_level`: 多层查询合并
//! - `compaction`: LSM Compaction 策略
//! - `index`: 统一的 FreshDiskANN 索引接口

pub mod fresh_graph;
pub mod sst;
pub mod multi_level;
pub mod compaction;
pub mod index;

pub use fresh_graph::{FreshVamanaGraph, FreshGraphConfig, VectorNode};
pub use sst::{VamanaSSTFile, SSTMetadata};
pub use multi_level::MultiLevelSearch;
pub use compaction::{CompactionStrategy, CompactionTrigger};
pub use index::{FreshDiskANNIndex, FreshDiskANNConfig};

/// 候选节点（用于搜索）
#[derive(Debug, Clone)]
pub struct Candidate {
    pub id: u64,
    pub distance: f32,
}

impl Candidate {
    pub fn new(id: u64, distance: f32) -> Self {
        Self { id, distance }
    }
}

impl PartialEq for Candidate {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for Candidate {}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 🔥 修复：最大堆（BinaryHeap 默认是最大堆，我们要距离大的在堆顶）
        // 这样 pop() 出来的是距离最大的，留在堆里的是距离最小的
        self.distance.partial_cmp(&other.distance).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
