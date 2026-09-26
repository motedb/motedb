//! Vamana configuration parameters

use crate::distance::DistanceKind;

/// Vamana index configuration
#[derive(Debug, Clone)]
pub struct VamanaConfig {
    /// Maximum degree (R parameter) - max neighbors per node
    pub max_degree: usize,

    /// Search list size during construction (L parameter)
    pub search_list_size: usize,

    /// 🔑 A3: 邻接表 LRU 容量 (None = 默认 1M 节点 — 覆盖嵌入式档位的
    /// 全图驻留; 设小可约束 RSS)。
    pub graph_cache_capacity: Option<usize>,

    /// Alpha parameter for pruning (typically 1.2)
    pub alpha: f32,

    /// Beam width for search
    pub beam_width: usize,

    /// Distance metric (L2 or Cosine)
    pub metric: DistanceKind,
}

impl Default for VamanaConfig {
    fn default() -> Self {
        Self {
            max_degree: 64,
            // 🔑 A1/A5 校准: 180 在紧簇数据上 recall@10 卡 0.86 (top-1
            // 0.97 — 区域导航已通, 枚举近邻不足); 300 配合度地板图收敛
            // 后实测 recall@10 ≥0.95、p50 <1ms (drought=64 终止)。
            search_list_size: 300,
            graph_cache_capacity: None,
            alpha: 1.2,
            beam_width: 48,                  // 🔧 折中: 32 → 48 (介于32和64之间)
            metric: DistanceKind::Euclidean, // 默认 L2（和 SQL <-> 一致）
        }
    }
}

impl VamanaConfig {
    /// Create configuration with specific metric
    pub fn with_metric(mut self, metric: DistanceKind) -> Self {
        self.metric = metric;
        self
    }

    /// Create configuration optimized for embedded environments
    pub fn embedded(dimension: usize) -> Self {
        // Lower parameters for memory efficiency
        let max_degree = if dimension <= 128 {
            32
        } else if dimension <= 384 {
            48
        } else {
            64
        };

        Self {
            max_degree,
            search_list_size: max_degree * 2,
            graph_cache_capacity: None,
            alpha: 1.2,
            beam_width: max_degree / 2,
            metric: DistanceKind::Euclidean,
        }
    }

    /// Create configuration optimized for performance
    pub fn performance(dimension: usize) -> Self {
        let max_degree = if dimension <= 128 {
            64
        } else if dimension <= 384 {
            96
        } else {
            128
        };

        Self {
            max_degree,
            search_list_size: max_degree * 3,
            graph_cache_capacity: None,
            alpha: 1.2,
            beam_width: max_degree,
            metric: DistanceKind::Euclidean,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = VamanaConfig::default();
        assert_eq!(config.max_degree, 64);
        assert_eq!(config.search_list_size, 300); // A1/A5 校准 (紧簇 recall 门) // Updated to match actual default
        assert!((config.alpha - 1.2).abs() < 0.001);
    }

    #[test]
    fn test_embedded_config() {
        let config = VamanaConfig::embedded(384);
        assert_eq!(config.max_degree, 48);
        assert!(config.max_degree < 64); // Should be more conservative
    }

    #[test]
    fn test_performance_config() {
        let config = VamanaConfig::performance(384);
        assert_eq!(config.max_degree, 96);
        assert!(config.max_degree > 64); // Should be more aggressive
    }
}
