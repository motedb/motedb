//! Compaction 策略

use super::VamanaSSTFile;

/// Compaction 触发器配置
#[derive(Debug, Clone)]
pub struct CompactionTrigger {
    /// L1 SST 文件数量阈值
    pub l1_threshold: usize,
}

impl Default for CompactionTrigger {
    fn default() -> Self {
        Self {
            l1_threshold: 4,  // 当 L1 有 4 个 SST 文件时触发合并
        }
    }
}

/// Compaction 策略
pub struct CompactionStrategy {
    config: CompactionTrigger,
}

impl CompactionStrategy {
    pub fn new(config: CompactionTrigger) -> Self {
        Self { config }
    }
    
    /// 判断是否需要 compaction
    pub fn should_compact(&self, l1_ssts: &[VamanaSSTFile]) -> bool {
        l1_ssts.len() >= self.config.l1_threshold
    }
}
